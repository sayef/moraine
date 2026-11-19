//! An atomic file replace (D48): remove some files and add others in one commit.

use std::collections::BTreeMap;

use arrow::array::RecordBatch;
use futures::StreamExt;
use futures::stream::BoxStream;

use super::Table;
use super::mutate::ReplaceWork;
use super::write::Pending;
use crate::error::{Error, Result};
use crate::options::Committed;
use crate::schema::conform;

/// One atomic swap of files, for [`Table::replace_files`].
///
/// It removes the named files and adds files written from the rows given, in one commit. The
/// commit conflicts only with another commit that removed one of the same files, and then
/// returns [`Error::FilesChanged`] with nothing written; files that other writers added
/// meanwhile stay. This is the primitive for folding many small files into a few, or a file of
/// statistics into a new one. moraine does not check the new rows against the removed ones:
/// when the table enforces its primary key, the rows written must not duplicate a key held by
/// another file.
#[derive(Default)]
pub struct Replace {
    pub(crate) remove: Vec<String>,
    pub(crate) batches: Vec<RecordBatch>,
    pub(crate) streams: Vec<BoxStream<'static, Result<RecordBatch>>>,
    pub(crate) tags: BTreeMap<String, String>,
    pub(crate) rows_unchanged: bool,
    pub(crate) fence: Option<crate::log::TxnId>,
}

impl std::fmt::Debug for Replace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replace")
            .field("remove", &self.remove)
            .field("batches", &self.batches.len())
            .field("streams", &self.streams.len())
            .field("tags", &self.tags)
            .field("rows_unchanged", &self.rows_unchanged)
            .field("fence", &self.fence)
            .finish()
    }
}

impl Replace {
    /// An empty replace: nothing removed, nothing added.
    pub fn new() -> Replace {
        Replace::default()
    }

    /// Removes the file at `path`, as [`crate::SnapshotFile::path`] names it.
    pub fn remove(mut self, path: impl Into<String>) -> Replace {
        self.remove.push(path.into());
        self
    }

    /// Removes every file in `paths`.
    pub fn remove_all<I, S>(mut self, paths: I) -> Replace
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.remove.extend(paths.into_iter().map(Into::into));
        self
    }

    /// Adds rows held in memory, written as data files of the table's layout: sorted by its
    /// cluster and primary key, split at the target file size.
    pub fn add_rows(mut self, batches: Vec<RecordBatch>) -> Replace {
        self.batches.extend(batches);
        self
    }

    /// Adds the rows of `stream`, read as the replace runs and written as data files one target
    /// file size at a time, so the rows are never all in memory (D68). Each file is sorted by
    /// the table's cluster and primary key on its own; the files follow the stream's order. An
    /// error from the stream fails the replace, and the files written for it are deleted.
    ///
    /// The files are uploaded before the commit, so the commit deadline (a quarter of the
    /// grace period, 15 minutes by default) runs from the first of them: a stream read for
    /// longer fails with [`Error::CommitDeadlineExceeded`]. Not for tables with blob columns.
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table, old: Vec<String>) -> moraine::Result<()> {
    /// use futures::StreamExt;
    /// // Rewrites the files in `old` without the rows whose `k` is "gone", file by file.
    /// let snap = t.snapshot().await?;
    /// let kept = snap
    ///     .scan()
    ///     .files(old.clone())
    ///     .filter(moraine::expr::col("k").not_eq(moraine::expr::lit("gone")))
    ///     .stream();
    /// t.replace_files(moraine::Replace::new().remove_all(old).add_stream(kept))
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn add_stream<S>(mut self, stream: S) -> Replace
    where
        S: futures::Stream<Item = Result<RecordBatch>> + Send + 'static,
    {
        self.streams.push(stream.boxed());
        self
    }

    /// Adds the batches of `batches`, consumed like [`Replace::add_stream`]: one target file
    /// size in memory at a time.
    pub fn add_batches<I>(self, batches: I) -> Replace
    where
        I: IntoIterator<Item = RecordBatch>,
        I::IntoIter: Send + 'static,
    {
        self.add_stream(futures::stream::iter(batches.into_iter().map(Ok)))
    }

    /// Tags the added files with `key = value` in the Delta `add.tags`; read them back with
    /// [`crate::SnapshotFile::tags`]. Keys starting with `moraine.` are reserved.
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Replace {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Fences the commit with `lease` (D29): it is refused with [`Error::Fenced`] once a newer
    /// generation of the same lease has committed, so a holder that lost its lease without
    /// noticing cannot commit over the new holder's work.
    pub fn fenced(mut self, lease: &crate::Lease) -> Replace {
        self.fence = Some(lease.fence());
        self
    }

    /// Declares that the added rows are exactly the removed files' rows (a rewrite for layout
    /// only). The commit then records no data change, and the new files take the place of the
    /// first removed file in the table's file order, as compaction's do, so rows with equal keys
    /// keep their commit order.
    pub fn rows_unchanged(mut self) -> Replace {
        self.rows_unchanged = true;
        self
    }
}

impl Table {
    /// Removes and adds files in one atomic, conflict-checked commit (see [`Replace`]).
    /// Returns the committed version, or the current one when the replace is empty.
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table, folded: arrow::array::RecordBatch) -> moraine::Result<()> {
    /// use moraine::Replace;
    /// let snap = t.snapshot().await?;
    /// let small: Vec<String> = snap
    ///     .files()
    ///     .into_iter()
    ///     .filter(|f| f.size < 1 << 20)
    ///     .map(|f| f.path)
    ///     .collect();
    /// match t
    ///     .replace_files(Replace::new().remove_all(small).add_rows(vec![folded]).tag("kind", "stats"))
    ///     .await
    /// {
    ///     Ok(c) => println!("replaced in version {}", c.version),
    ///     // Another writer removed one of the files first; plan again on a fresh snapshot.
    ///     Err(moraine::Error::FilesChanged(path)) => println!("{path} changed meanwhile"),
    ///     Err(e) => return Err(e),
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn replace_files(&self, r: Replace) -> Result<Committed> {
        if let Some(k) = r.tags.keys().find(|k| k.starts_with("moraine.")) {
            return Err(Error::InvalidQuery(format!("tag key {k:?} is reserved")));
        }
        let snap = self.inner.current();
        let def = &snap.def;
        let batches: Vec<RecordBatch> = r
            .batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .map(|b| conform(b, snap.schema(), def))
            .collect::<Result<_>>()?;
        let mut keys = def
            .enforce_primary_key
            .then(std::collections::HashSet::<Vec<u8>>::new);
        if let Some(seen) = &mut keys {
            crate::schema::check_unique_keys_with(&batches, &def.primary_key, seen)?;
        }
        let (ready, started) = self
            .inner
            .upload_streams(r.streams, &r.tags, keys.as_mut())
            .await?;
        if r.remove.is_empty() && batches.is_empty() && ready.is_empty() && r.fence.is_none() {
            return Ok(Committed::new(snap.version));
        }
        let work = ReplaceWork {
            operation: if r.rows_unchanged {
                "OPTIMIZE"
            } else {
                "REPLACE"
            },
            remove: r.remove,
            batches,
            ready,
            tags: r.tags,
            data_change: !r.rows_unchanged,
            fence: r.fence,
            started,
            txns: Vec::new(),
        };
        let outcome = self
            .inner
            .enqueue(Pending::Replace(Box::new(work)), None)
            .await?;
        Ok(Committed::new(outcome.version()))
    }
}

impl super::TableInner {
    /// Commits a replace through the handle's queue, after the writes queued before it.
    pub(crate) async fn commit_replace(
        self: &std::sync::Arc<Self>,
        work: ReplaceWork,
    ) -> Result<u64> {
        Ok(self
            .enqueue(Pending::Replace(Box::new(work)), None)
            .await?
            .version())
    }
}

impl super::TableInner {
    /// Reads `streams` and uploads their rows as data files of about the target size, each
    /// sorted on its own and with its sidecars built before the commit, tagged with `tags`.
    /// Returns the files and when the first upload began (the commit clock starts there). On
    /// an error the files uploaded so far are deleted.
    pub(crate) async fn upload_streams(
        self: &std::sync::Arc<Self>,
        streams: Vec<BoxStream<'static, Result<RecordBatch>>>,
        tags: &BTreeMap<String, String>,
        mut keys: Option<&mut std::collections::HashSet<Vec<u8>>>,
    ) -> Result<(Vec<crate::log::DataFile>, Option<std::time::Duration>)> {
        if streams.is_empty() {
            return Ok((Vec::new(), None));
        }
        let snap = self.current();
        if !crate::blob::columns(snap.schema()).is_empty() {
            return Err(Error::InvalidQuery(
                "a replace from a stream does not take tables with blob columns; use add_rows"
                    .into(),
            ));
        }
        let uploads = std::sync::Mutex::new(super::mutate::Uploads::default());
        let mut started = None;
        let mut out = Vec::new();
        let r = async {
            let target = snap.def.target_file_bytes;
            let mut buf: Vec<RecordBatch> = Vec::new();
            let mut bytes = 0u64;
            for mut s in streams {
                loop {
                    let next = s.next().await.transpose()?;
                    let last = next.is_none();
                    if let Some(b) = next.filter(|b| b.num_rows() > 0) {
                        let b = conform(&b, snap.schema(), &snap.def)?;
                        if let Some(seen) = keys.as_deref_mut() {
                            crate::schema::check_unique_keys_with(
                                std::slice::from_ref(&b),
                                &snap.def.primary_key,
                                seen,
                            )?;
                        }
                        bytes += b.get_array_memory_size() as u64;
                        buf.push(b);
                    }
                    if !buf.is_empty() && (bytes >= target || last) {
                        let sorted = crate::write::sort_rows(snap.schema(), &snap.def, &buf)?;
                        buf.clear();
                        bytes = 0;
                        started.get_or_insert(self.db.clock().now());
                        let (files, _) = self
                            .upload(
                                &snap,
                                sorted,
                                crate::index::inline::Writer::Append,
                                Some(&uploads),
                            )
                            .await?;
                        out.extend(files.into_iter().map(|mut f| {
                            f.tags.extend(tags.clone());
                            f
                        }));
                    }
                    if last {
                        break;
                    }
                }
            }
            Ok::<_, Error>(())
        }
        .await;
        if let Err(e) = r {
            let files = uploads
                .into_inner()
                .unwrap_or_else(|p| p.into_inner())
                .files();
            self.delete_objects(&files, &[]).await;
            return Err(e);
        }
        Ok((out, started))
    }
}
