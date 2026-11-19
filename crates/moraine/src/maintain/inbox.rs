//! Queued writes (D29, D51): writers put batches into an inbox, one ingestor commits them.
//!
//! [`crate::Table::submit`] writes a batch as one Arrow IPC object,
//! `meta/inbox/<submitter>/<sequence>.arrow`, with put-if-absent: one uncontended PUT, however
//! many writers there are. The submitter id is per table handle; its sequence numbers have no
//! gaps, because a handle submits one batch at a time and reuses a number only when the put
//! surely did not happen (a put whose outcome is unknown is put again, unchanged, before the
//! next one).
//!
//! The ingestor ([`crate::Table::ingest`], or a maintenance pass) holds the `ingest` lease. It
//! lists the inbox, takes for each submitter the batches after the newest sequence the log
//! records for it, in order and without gaps, and commits them as one append whose entry carries
//! a Delta transaction per submitter (`appId` = submitter, `version` = its last sequence) and
//! the lease generation as a fence. A batch is therefore applied exactly once: a second
//! ingestor, a retried commit or a crash between commit and cleanup finds the sequence already
//! recorded. Committed batches are then deleted. A batch that cannot be decoded or no longer
//! fits the schema is copied to `meta/inbox-rejected/` and its sequence recorded as applied, so
//! the batches after it are not held up.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};

use crate::error::{Error, Result};
use crate::log::TxnId;
use crate::schema::conform;
use crate::table::{ReplaceWork, Table, TableInner};

/// A batch accepted into the inbox ([`crate::Table::submit`]).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Submitted {
    /// The submitting handle's id; also the Delta transaction application id of its batches.
    pub submitter: String,
    /// The batch's number among the submitter's batches; also its transaction version.
    pub sequence: i64,
}

/// What one ingest round did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct IngestReport {
    /// The version that committed the batches.
    pub version: Option<u64>,
    /// Batches committed.
    pub batches: usize,
    /// Rows in those batches.
    pub rows: u64,
    /// Submitters the batches came from.
    pub submitters: usize,
    /// Batches set aside under `meta/inbox-rejected/`.
    pub rejected: usize,
    /// Inbox objects of batches committed earlier, deleted now.
    pub cleaned: usize,
    /// Another ingestor committed some of the batches first; nothing was written this round.
    pub yielded: bool,
}

/// Submission state of one handle.
#[derive(Debug, Default)]
pub(crate) struct SubmitState {
    /// The next sequence number (sequences start at 1).
    next: i64,
    /// A put whose outcome is unknown: it is put again before anything else.
    unsure: Option<(i64, Bytes)>,
}

fn encode(batches: &[RecordBatch]) -> Result<Bytes> {
    let schema = batches
        .first()
        .map(|b| b.schema())
        .ok_or_else(|| Error::Internal("empty submission".into()))?;
    let mut out = Vec::new();
    let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut out, &schema)?;
    for b in batches {
        w.write(b)?;
    }
    w.finish()?;
    drop(w);
    Ok(Bytes::from(out))
}

/// Decodes an inbox object. A corrupt object is an error, never an abort: its framing is
/// checked first (arrow's reader allocates a message body by the length the message states,
/// and T13 found an object that asked for 256 PiB), and a panic inside arrow's decoder becomes
/// an error too.
pub(crate) fn decode(body: Bytes) -> Result<Vec<RecordBatch>> {
    check_frames(&body)?;
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let r = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(body), None)?;
        Ok(r.collect::<std::result::Result<Vec<_>, _>>()?)
    }))
    .unwrap_or_else(|_| Err(ipc_error("the decoder panicked")))
}

fn ipc_error(m: &str) -> Error {
    Error::from(arrow::error::ArrowError::IpcError(format!(
        "queued batch: {m}"
    )))
}

/// Checks that every message of an Arrow IPC stream (its metadata and its body) lies inside
/// `b`, before the reader allocates anything by a length the stream states.
fn check_frames(b: &[u8]) -> Result<()> {
    let word = |at: usize| -> Option<i32> {
        b.get(at..at + 4)
            .map(|w| i32::from_le_bytes([w[0], w[1], w[2], w[3]]))
    };
    let mut at = 0usize;
    while at < b.len() {
        let mut len = word(at).ok_or_else(|| ipc_error("truncated message length"))?;
        at += 4;
        if len == -1 {
            // The continuation marker; the length follows.
            len = word(at).ok_or_else(|| ipc_error("truncated message length"))?;
            at += 4;
        }
        if len == 0 {
            return Ok(());
        }
        let len = usize::try_from(len).map_err(|_| ipc_error("negative message length"))?;
        let meta = b
            .get(at..at.saturating_add(len))
            .ok_or_else(|| ipc_error("message metadata beyond the end of the object"))?;
        at += len;
        let msg = arrow::ipc::root_as_message(meta)
            .map_err(|e| ipc_error(&format!("message metadata: {e}")))?;
        let body = usize::try_from(msg.bodyLength())
            .map_err(|_| ipc_error("negative message body length"))?;
        if body > b.len() - at {
            return Err(ipc_error("message body beyond the end of the object"));
        }
        at += body;
    }
    Ok(())
}

impl TableInner {
    fn inbox(&self) -> Path {
        self.meta_prefix().join("inbox")
    }

    fn submitter(&self) -> String {
        format!("moraine.inbox.{}", self.db.writer_id)
    }

    async fn put_inbox(&self, sequence: i64, body: Bytes) -> Result<()> {
        let at = self
            .inbox()
            .join(self.submitter().as_str())
            .join(format!("{sequence:020}.arrow"));
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .db
            .store
            .object_store()
            .put_opts(&at, PutPayload::from(body), opts)
            .await
        {
            // The name is this handle's alone: an existing object is an earlier attempt of
            // this very put.
            Ok(_)
            | Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn submit_queued(&self, batches: Vec<RecordBatch>) -> Result<Submitted> {
        self.db.require_writable().await?;
        let snap = self.current();
        if snap.def.enforce_primary_key {
            return Err(Error::PrimaryKeyEnforced);
        }
        let conformed: Vec<RecordBatch> = batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .map(|b| conform(b, snap.schema(), &snap.def))
            .collect::<Result<_>>()?;
        if conformed.is_empty() {
            return Err(Error::InvalidBatch("nothing to submit".into()));
        }
        // The ingestor copies any reference it did not upload, possibly after the referenced
        // object was collected: queued writes take blob values as bytes.
        let mut refs = std::collections::HashSet::new();
        for b in &conformed {
            crate::blob::keys_of(b, &mut refs)?;
        }
        if !refs.is_empty() {
            return Err(Error::InvalidBatch(
                "queued writes take blob values as bytes, not as references".into(),
            ));
        }
        let body = encode(&conformed)?;
        let mut st = self.submit_state.lock().await;
        if let Some((seq, unsure)) = st.unsure.clone() {
            self.put_inbox(seq, unsure).await?;
            st.unsure = None;
        }
        let seq = st.next.max(1);
        fail::fail_point!("moraine::submit::before_put");
        match self.put_inbox(seq, body.clone()).await {
            Ok(()) => {
                st.next = seq + 1;
                Ok(Submitted {
                    submitter: self.submitter(),
                    sequence: seq,
                })
            }
            Err(Error::Storage(e))
                if crate::commit::AttemptError::from_store_is_open(e.inner()) =>
            {
                // It may have landed: keep the number taken and put the same bytes again first
                // next time, so the sequence never has a gap.
                st.unsure = Some((seq, body));
                st.next = seq + 1;
                Err(Error::OutcomeUnknown {
                    txn: format!("{}:{seq}", self.submitter()),
                })
            }
            Err(e) => Err(e),
        }
    }

    /// One ingest round, fenced by `fence` when a lease is held. Commits at most `max`
    /// batches.
    pub(crate) async fn ingest(
        self: &Arc<Self>,
        fence: Option<TxnId>,
        max: usize,
    ) -> Result<IngestReport> {
        let mut report = IngestReport::default();
        let snap = self.refresh().await?;
        let store = self.db.store.object_store().clone();
        let inbox = self.inbox();
        let listed: Vec<_> = store.list(Some(&inbox)).try_collect().await?;
        // submitter -> sequence -> location
        let mut found: BTreeMap<String, BTreeMap<i64, Path>> = BTreeMap::new();
        for m in listed {
            let Some(rel) = m.location.prefix_match(&inbox) else {
                continue;
            };
            let parts: Vec<String> = rel.map(|p| p.as_ref().to_string()).collect();
            let [submitter, name] = parts.as_slice() else {
                continue;
            };
            let Some(seq) = name
                .strip_suffix(".arrow")
                .and_then(|s| s.parse::<i64>().ok())
            else {
                continue;
            };
            found
                .entry(submitter.clone())
                .or_default()
                .insert(seq, m.location);
        }
        let mut done: Vec<Path> = Vec::new();
        let mut take: Vec<(String, i64, Path)> = Vec::new();
        for (submitter, seqs) in &found {
            let applied = snap.txns.get(submitter).unwrap_or(0);
            let mut next = applied + 1;
            for (seq, loc) in seqs {
                if *seq <= applied {
                    done.push(loc.clone());
                } else if *seq == next && take.len() < max {
                    take.push((submitter.clone(), *seq, loc.clone()));
                    next += 1;
                }
            }
        }
        report.cleaned = delete_all(&store, &done).await?;
        if take.is_empty() {
            return Ok(report);
        }
        let mut batches: Vec<RecordBatch> = Vec::new();
        // Per submitter: the first and the last sequence this round commits.
        let mut last: BTreeMap<String, (i64, i64)> = BTreeMap::new();
        let mut taken: Vec<Path> = Vec::new();
        for (submitter, seq, loc) in take {
            let body = store.get(&loc).await?.bytes().await?;
            let ok = decode(body.clone()).and_then(|bs| {
                bs.iter()
                    .map(|b| conform(b, snap.schema(), &snap.def))
                    .collect::<Result<Vec<_>>>()
            });
            match ok {
                Ok(bs) => {
                    report.rows += bs.iter().map(|b| b.num_rows() as u64).sum::<u64>();
                    report.batches += 1;
                    batches.extend(bs);
                }
                Err(_) => {
                    let aside = self
                        .meta_prefix()
                        .join("inbox-rejected")
                        .join(submitter.as_str())
                        .join(format!("{seq:020}.arrow"));
                    store.put(&aside, PutPayload::from(body)).await?;
                    report.rejected += 1;
                }
            }
            last.entry(submitter).or_insert((seq, seq)).1 = seq;
            taken.push(loc);
        }
        report.submitters = last.len();
        let work = ReplaceWork {
            operation: "WRITE",
            remove: Vec::new(),
            batches,
            ready: Vec::new(),
            tags: Default::default(),
            data_change: true,
            fence,
            started: None,
            txns: last
                .into_iter()
                .map(|(a, (first, v))| (TxnId::new(a, v), first))
                .collect(),
        };
        fail::fail_point!("moraine::ingest::before_commit");
        match self.commit_replace(work).await {
            Ok(v) => report.version = Some(v),
            Err(Error::AlreadyIngested(_)) | Err(Error::Fenced { .. }) => {
                report.yielded = true;
                report.batches = 0;
                report.rows = 0;
                return Ok(report);
            }
            Err(e) => return Err(e),
        }
        fail::fail_point!("moraine::ingest::after_commit");
        report.cleaned += delete_all(&store, &taken).await?;
        Ok(report)
    }
}

async fn delete_all(store: &Arc<dyn object_store::ObjectStore>, paths: &[Path]) -> Result<usize> {
    let mut n = 0;
    for p in paths {
        match store.delete(p).await {
            Ok(()) => n += 1,
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(n)
}

impl Table {
    /// Queues rows for the ingestor instead of committing them (D51): one put-if-absent of an
    /// inbox object, which never contends with other writers. The rows become visible once an
    /// ingestor ([`Table::ingest`], or a maintenance pass) commits them, exactly once, in the
    /// order this handle submitted them. Appends only; a table that enforces its primary key
    /// refuses them ([`Error::PrimaryKeyEnforced`]).
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table, batch: arrow::array::RecordBatch) -> moraine::Result<()> {
    /// let ticket = t.submit(vec![batch]).await?;
    /// // Somewhere (any process): t.ingest(true).await?;
    /// while !t.is_ingested(&ticket).await? {
    ///     tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn submit(&self, batches: Vec<RecordBatch>) -> Result<Submitted> {
        self.inner.submit_queued(batches).await
    }

    /// Commits queued rows from the inbox, under the `ingest` lease when `lease` is true (then
    /// a round that finds the lease held elsewhere does nothing).
    pub async fn ingest(&self, lease: bool) -> Result<IngestReport> {
        let now = if lease {
            self.inner.server_time().await?
        } else {
            chrono::Utc::now()
        };
        let ttl = std::time::Duration::from_secs(300);
        match self.inner.turn("ingest", lease, ttl, now).await? {
            super::Turn::Run(l) => self.inner.ingest(l.map(|l| l.fence()), usize::MAX).await,
            super::Turn::Skip => Ok(IngestReport::default()),
        }
    }

    /// Whether a submitted batch is committed, after moving the handle to the newest version.
    pub async fn is_ingested(&self, s: &Submitted) -> Result<bool> {
        Ok(self
            .last_txn_version(&s.submitter)
            .await?
            .is_some_and(|v| v >= s.sequence))
    }
}
