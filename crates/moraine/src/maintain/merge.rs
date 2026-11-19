//! Streaming compaction (D59; D55's deferral): a k-way merge over input files already
//! sorted by the table's sort columns, written out a chunk at a time.
//!
//! Each input is read a row group at a time in batches of 1,024 rows. The merge takes, among
//! the inputs' current rows, the smallest by the sort columns, and on equal keys the input that
//! comes first in the snapshot's file order, so the result equals the stable sort of the
//! concatenated inputs that the in-memory path computes. Rows go to a Parquet writer on a local
//! temporary file and to the sidecar builders at once; an output file is cut at a row count
//! fixed before the merge (the input's rows spread evenly over the files the target size asks
//! for), and uploaded when it is complete. One pass reads at most [`FAN_IN`] inputs; more are
//! merged in groups into local files first. Memory is bounded by one row group per input of a
//! pass, the writer's open row group and the builders' budgets, not by the input's size.
//!
//! Index kinds that can merge sidecars without reading rows ([`IndexKind::merge`]; turbovec's
//! codes concatenate, D24) do so when every input has its sidecar under the current spec: the
//! merge records where each input row went, and the kind gathers the inputs' codes in the
//! output's row order.

use std::collections::HashMap;
use std::io::{BufWriter, Seek, Write};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::interleave_record_batch;
use arrow::row::{RowConverter, Rows, SortField};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::ObjectStoreExt;
use parquet::arrow::ArrowWriter;

use crate::error::{Error, Result};
use crate::index::build::{BundleBuild, RemoveOnDrop, upload};
use crate::index::bundle::{BundleBytes, BundleView, Expect};
use crate::index::discover::{Listed, sidecar_location};
use crate::index::inline::{InlineBuilds, Writer};
use crate::index::kind::{BuildCtx, IndexKind, MergeInput, RowMap};
use crate::index::spec::{IndexSpec, spec_hash};
use crate::log::DataFile;
use crate::snapshot::Snapshot;
use crate::stats::FileStats;
use crate::table::TableInner;

/// Rows per input batch.
const INPUT_BATCH: usize = 1024;
/// Rows per output chunk at most.
const CHUNK: usize = 8192;
/// Inputs one merge pass reads at once. Each holds a reader, a decoded batch and its row group's
/// column chunks; with thousands of small inputs (a day of flushes) one pass over all of them
/// held more than the in-memory merge, and comparing every input per row was quadratic.
const FAN_IN: usize = 32;

/// Where a merge pass reads an input from.
enum Source {
    Remote(Arc<DataFile>),
    /// A local file holding an earlier pass's merge of a group of inputs.
    Local(std::path::PathBuf),
}

/// A k-way merge over sorted row streams: equal keys come from the earlier stream.
struct Merger {
    inputs: Vec<Input>,
    conv: Option<RowConverter>,
    idx: Vec<usize>,
}

impl Merger {
    async fn new(
        streams: Vec<BoxStream<'static, Result<RecordBatch>>>,
        schema: &arrow::datatypes::SchemaRef,
        sort_cols: &[String],
    ) -> Result<Merger> {
        let conv = if sort_cols.is_empty() {
            None
        } else {
            let fields = sort_cols
                .iter()
                .map(|c| {
                    Ok(SortField::new(
                        schema.field_with_name(c)?.data_type().clone(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Some(RowConverter::new(fields)?)
        };
        let idx: Vec<usize> = sort_cols
            .iter()
            .map(|c| schema.index_of(c))
            .collect::<std::result::Result<_, _>>()?;
        let mut inputs: Vec<Input> = streams
            .into_iter()
            .map(|stream| Input {
                stream,
                batch: None,
                keys: None,
                pos: 0,
                base: 0,
            })
            .collect();
        for inp in inputs.iter_mut() {
            load(inp, conv.as_ref(), &idx).await?;
        }
        Ok(Merger { inputs, conv, idx })
    }

    /// Whether input `a`'s current row goes before input `b`'s.
    fn before(&self, a: usize, b: usize) -> bool {
        match (self.inputs[a].current(), self.inputs[b].current()) {
            (Some(x), Some(y)) => x < y || (x == y && a < b),
            _ => a < b,
        }
    }

    /// The input whose current row is next, and the one after it.
    fn pick(&self) -> (Option<usize>, Option<usize>) {
        let mut best: Option<usize> = None;
        let mut second: Option<usize> = None;
        for i in 0..self.inputs.len() {
            if !self.inputs[i].has_row() {
                continue;
            }
            // Without sort columns the inputs are concatenated in order.
            if self.conv.is_none() {
                return (Some(i), None);
            }
            match best {
                None => best = Some(i),
                Some(b) if self.before(i, b) => {
                    second = best;
                    best = Some(i);
                }
                Some(_) => {
                    if second.is_none_or(|s| self.before(i, s)) {
                        second = Some(i);
                    }
                }
            }
        }
        (best, second)
    }

    /// Up to `max` next rows, with the input and the offset in that input each came from;
    /// `None` at the end. A chunk ends early where an input's batch runs out.
    async fn next(&mut self, max: usize) -> Result<Option<(RecordBatch, Vec<(usize, u64)>)>> {
        let mut chunk: Vec<(usize, usize)> = Vec::new();
        let mut from: Vec<(usize, u64)> = Vec::new();
        let mut exhausted = None;
        'outer: while chunk.len() < max {
            let (best, second) = self.pick();
            let Some(b) = best else { break };
            // Take a run from `b` while it stays ahead of the runner-up.
            loop {
                let inp = &mut self.inputs[b];
                chunk.push((b, inp.pos));
                from.push((b, inp.base + inp.pos as u64));
                inp.pos += 1;
                if !inp.has_row() {
                    exhausted = Some(b);
                    break 'outer;
                }
                if chunk.len() >= max {
                    break 'outer;
                }
                if let Some(s) = second
                    && !self.before(b, s)
                {
                    break;
                }
            }
        }
        let Some(rows) = take_chunk(&self.inputs, &mut chunk)? else {
            return Ok(None);
        };
        if let Some(b) = exhausted {
            load(&mut self.inputs[b], self.conv.as_ref(), &self.idx).await?;
        }
        Ok(Some((rows, from)))
    }
}

/// One input's cursor.
struct Input {
    stream: BoxStream<'static, Result<RecordBatch>>,
    batch: Option<RecordBatch>,
    keys: Option<Rows>,
    pos: usize,
    /// Rows of this input consumed before `batch`.
    base: u64,
}

impl Input {
    fn current(&self) -> Option<&[u8]> {
        self.keys.as_ref().map(|k| k.row(self.pos).data())
    }

    fn has_row(&self) -> bool {
        self.batch.as_ref().is_some_and(|b| self.pos < b.num_rows())
    }
}

/// A sidecar merged from the inputs' sidecars.
struct Merged {
    spec: IndexSpec,
    kind: Arc<dyn IndexKind>,
    hash: String,
    inputs: Vec<BundleView>,
    /// Per input, where its rows went in the current output file.
    maps: Vec<RowMap>,
}

/// The output file being written.
struct Output {
    name: String,
    tmp: RemoveOnDrop,
    writer: ArrowWriter<BufWriter<std::fs::File>>,
    rows: u64,
    stats: FileStats,
    builds: InlineBuilds,
    /// Time spent feeding `builds`, by the database clock (D81).
    build_time: std::time::Duration,
}

/// What streaming produced.
pub(crate) struct Streamed {
    pub files: Vec<DataFile>,
    pub sidecars: usize,
    pub merged_sidecars: usize,
}

impl TableInner {
    /// Whether `files` can be merged by streaming: every one records its row count and is
    /// sorted by the table's current sort columns (or the table has none).
    pub(crate) fn can_stream(snap: &Snapshot, files: &[Arc<DataFile>]) -> bool {
        let cols = snap.def.sort_columns();
        files.iter().all(|f| {
            f.rows().is_some()
                && (cols.is_empty() || f.sorted_by.as_ref().is_some_and(|s| *s == cols))
        })
    }

    /// Merges `files` (adjacent, in snapshot order) into new data files of `snap`'s layout by
    /// streaming, with their sidecars. See the module documentation.
    pub(crate) async fn merge_streaming(
        self: &Arc<Self>,
        snap: &Arc<Snapshot>,
        files: &[Arc<DataFile>],
        writer: Writer,
    ) -> Result<Streamed> {
        let schema = snap.schema().clone();
        let def = &snap.def;
        let total_rows: u64 = files.iter().filter_map(|f| f.rows()).sum();
        let total_bytes: u64 = files.iter().map(|f| f.size).sum();
        let outputs = total_bytes.div_ceil(def.target_file_bytes.max(1)).max(1);
        let per_output = total_rows.div_ceil(outputs).max(1);

        // Kinds that merge sidecars, when every input has one.
        let specs = self.inline_specs(snap, per_output, writer)?;
        let mut merged: Vec<Merged> = Vec::new();
        let mut rebuilt: Vec<(IndexSpec, Arc<dyn IndexKind>)> = Vec::new();
        for (spec, kind) in specs {
            // Row maps cover one pass only.
            if files.len() > FAN_IN {
                rebuilt.push((spec, kind));
                continue;
            }
            let m = match self.merge_inputs(&spec, &kind, files).await? {
                Some(inputs) if !inputs.is_empty() => {
                    let probe: Vec<MergeInput<'_>> = Vec::new();
                    // A kind without a merge says so for an empty input list too.
                    let ctx = BuildCtx {
                        source_rows: 0,
                        budget_bytes: self.db.index.cfg.build_budget,
                        threads: 1,
                    };
                    kind.merge(&spec, &probe, &ctx)?.map(|_| inputs)
                }
                _ => None,
            };
            match m {
                Some(inputs) => {
                    let hash = spec_hash(kind.as_ref(), &spec);
                    merged.push(Merged {
                        maps: inputs
                            .iter()
                            .zip(files)
                            .map(|(_, f)| RowMap {
                                new: vec![None; f.rows().unwrap_or(0) as usize],
                            })
                            .collect(),
                        spec,
                        kind,
                        hash,
                        inputs,
                    })
                }
                None => rebuilt.push((spec, kind)),
            }
        }
        let _permit = if rebuilt.is_empty() {
            None
        } else {
            Some(
                self.db
                    .index
                    .reserve_build(self.db.index.cfg.build_budget.saturating_mul(rebuilt.len()))
                    .await?,
            )
        };

        let sort_cols = def.sort_columns();
        // More inputs than one pass reads at once: merge groups of adjacent inputs into local
        // files first (equal keys still go to the earlier group, so the order is the same).
        let mut sources: Vec<Source> = files.iter().map(|f| Source::Remote(f.clone())).collect();
        let mut temps: Vec<RemoveOnDrop> = Vec::new();
        while sources.len() > FAN_IN {
            let mut next = Vec::with_capacity(sources.len().div_ceil(FAN_IN));
            let mut it = std::mem::take(&mut sources).into_iter().peekable();
            while it.peek().is_some() {
                let group: Vec<Source> = it.by_ref().take(FAN_IN).collect();
                if group.len() == 1 {
                    next.extend(group);
                    continue;
                }
                let streams = self.open_sources(&group, &schema).await?;
                let mut m = Merger::new(streams, &schema, &sort_cols).await?;
                let tmp = RemoveOnDrop(self.db.index.build_dir().join(format!(
                    "merge-{}.parquet",
                    crate::clock::random_name(&crate::clock::SystemClock::new())
                )));
                let props = parquet::file::properties::WriterProperties::builder()
                    .set_max_row_group_row_count(Some(16_384))
                    .build();
                let mut w = ArrowWriter::try_new(
                    BufWriter::with_capacity(1 << 20, std::fs::File::create(&tmp.0)?),
                    schema.clone(),
                    Some(props),
                )?;
                while let Some((rows, _)) = m.next(CHUNK).await? {
                    w.write(&rows)?;
                }
                w.close()?;
                next.push(Source::Local(tmp.0.clone()));
                temps.push(tmp);
            }
            sources = next;
        }
        let streams = self.open_sources(&sources, &schema).await?;
        let mut m = Merger::new(streams, &schema, &sort_cols).await?;

        let mut out = Streamed {
            files: Vec::new(),
            sidecars: 0,
            merged_sidecars: 0,
        };
        let mut cur: Option<Output> = None;
        let mut emitted_total = 0u64;
        loop {
            let in_file = cur.as_ref().map_or(0, |o| o.rows);
            let room = per_output.saturating_sub(in_file).max(1) as usize;
            let Some((rows, from)) = m.next(CHUNK.min(room)).await? else {
                break;
            };
            // Where each input row went, for kinds that merge sidecars.
            for mg in &mut merged {
                for (j, (i, old)) in from.iter().enumerate() {
                    if let Some(slot) = mg.maps[*i].new.get_mut(*old as usize) {
                        *slot = Some(in_file + j as u64);
                    }
                }
            }
            emitted_total += rows.num_rows() as u64;
            self.write_chunk(snap, Some(rows), &mut cur, &rebuilt, per_output)
                .await?;
            let full = cur.as_ref().is_some_and(|o| o.rows >= per_output);
            if full
                && emitted_total < total_rows
                && let Some(o) = cur.take()
            {
                let (f, s, mm) = self.close_output(snap, o, &mut merged, files).await?;
                out.files.push(f);
                out.sidecars += s;
                out.merged_sidecars += mm;
            }
        }
        if let Some(o) = cur.take() {
            let (f, s, mm) = self.close_output(snap, o, &mut merged, files).await?;
            out.files.push(f);
            out.sidecars += s;
            out.merged_sidecars += mm;
        }
        drop(temps);
        if emitted_total != total_rows {
            return Err(Error::Internal(format!(
                "streaming compaction read {emitted_total} rows, the files record {total_rows}"
            )));
        }
        Ok(out)
    }

    /// Opens the row streams of `sources`.
    async fn open_sources(
        &self,
        sources: &[Source],
        schema: &arrow::datatypes::SchemaRef,
    ) -> Result<Vec<BoxStream<'static, Result<RecordBatch>>>> {
        let store = self.db.store.object_store().clone();
        let data = self.layer.data_prefix().clone();
        let mut out = Vec::with_capacity(sources.len());
        for s in sources {
            out.push(match s {
                Source::Remote(f) => {
                    let path = data.clone().join(f.path.as_str());
                    crate::read::stream_file(
                        &self.db.cache,
                        &store,
                        &path,
                        f.size,
                        schema,
                        INPUT_BATCH,
                    )
                    .await?
                }
                Source::Local(p) => {
                    let r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                        std::fs::File::open(p)?,
                    )?
                    .with_batch_size(INPUT_BATCH)
                    .build()?;
                    futures::stream::iter(r.map(|b| b.map_err(Error::from))).boxed()
                }
            });
        }
        Ok(out)
    }

    /// The inputs' sidecars of `spec`, opened, when every input has one; `None` otherwise.
    async fn merge_inputs(
        &self,
        spec: &IndexSpec,
        kind: &Arc<dyn IndexKind>,
        files: &[Arc<DataFile>],
    ) -> Result<Option<Vec<BundleView>>> {
        let hash = spec_hash(kind.as_ref(), spec);
        let store = self.db.store.object_store();
        let idx = self.idx_prefix();
        let mut views = Vec::with_capacity(files.len());
        for f in files {
            if f.rows().is_some_and(|r| !kind.wants_sidecar(spec, r)) {
                return Ok(None);
            }
            let at = sidecar_location(&idx, &f.path, &spec.name, &hash);
            let body = match store.get(&at).await {
                Ok(g) => g.bytes().await?,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            let Ok(view) = BundleBytes::copy(&body)
                .map_err(|e| e.to_string())
                .and_then(|b| BundleView::parse(b).map_err(|e| e.to_string()))
            else {
                return Ok(None);
            };
            let ok = view
                .check(&Expect {
                    kind: kind.name(),
                    kind_format: kind.format_version(),
                    spec_hash: &hash,
                    source_rows: f.rows(),
                    source_size: f.size,
                })
                .and_then(|()| view.verify_sections())
                .is_ok();
            if !ok {
                return Ok(None);
            }
            views.push(view);
        }
        Ok(Some(views))
    }

    /// Writes the chunk's rows to the current output file (opening one if needed) and to its
    /// sidecar builders.
    async fn write_chunk(
        &self,
        snap: &Snapshot,
        rows: Option<RecordBatch>,
        cur: &mut Option<Output>,
        rebuilt: &[(IndexSpec, Arc<dyn IndexKind>)],
        per_output: u64,
    ) -> Result<()> {
        let Some(rows) = rows else {
            return Ok(());
        };
        if cur.is_none() {
            *cur = Some(self.open_output(snap, rebuilt, per_output)?);
        }
        let Some(o) = cur.as_mut() else {
            return Ok(());
        };
        o.writer.write(&rows)?;
        o.stats.merge(&crate::stats::compute(&rows)?);
        if !o.builds.is_empty() {
            let first = o.rows;
            let mut builds = std::mem::replace(&mut o.builds, InlineBuilds::empty());
            let r2 = rows.clone();
            let clock = self.db.clock().clone();
            let t0 = clock.now();
            builds = tokio::task::spawn_blocking(move || -> Result<InlineBuilds> {
                builds.add(first, &r2)?;
                Ok(builds)
            })
            .await
            .map_err(|e| Error::Internal(format!("compaction build task: {e}")))??;
            o.builds = builds;
            o.build_time += clock.now().saturating_sub(t0);
        }
        o.rows += rows.num_rows() as u64;
        Ok(())
    }

    fn open_output(
        &self,
        snap: &Snapshot,
        rebuilt: &[(IndexSpec, Arc<dyn IndexKind>)],
        per_output: u64,
    ) -> Result<Output> {
        let clock = self.db.clock().clone();
        let name = format!("part-{}.parquet", crate::clock::random_name(clock.as_ref()));
        let tmp = RemoveOnDrop(self.db.index.build_dir().join(format!(
            "compact-{}.parquet",
            crate::clock::random_name(&crate::clock::SystemClock::new())
        )));
        let file = std::fs::File::create(&tmp.0)?;
        let props = crate::write::writer_props(snap.schema(), &snap.def, per_output as usize);
        let writer = ArrowWriter::try_new(
            BufWriter::with_capacity(1 << 20, file),
            snap.schema().clone(),
            Some(props),
        )?;
        let builds = self.start_inline(snap, rebuilt.to_vec(), per_output)?;
        Ok(Output {
            name,
            tmp,
            writer,
            rows: 0,
            stats: FileStats {
                rows: Some(0),
                columns: HashMap::new(),
            },
            builds,
            build_time: std::time::Duration::ZERO,
        })
    }

    /// Closes `o`, uploads it and its sidecars, and resets the merge maps for the next file.
    async fn close_output(
        &self,
        snap: &Snapshot,
        o: Output,
        merged: &mut [Merged],
        files: &[Arc<DataFile>],
    ) -> Result<(DataFile, usize, usize)> {
        let Output {
            name,
            tmp,
            writer,
            rows,
            stats,
            builds,
            build_time,
        } = o;
        if !builds.is_empty() {
            self.db
                .index
                .table(self.idx_prefix().as_ref())
                .record_build(rows, build_time);
        }
        let mut w = writer.into_inner()?;
        w.flush()?;
        let size = w.stream_position()?;
        drop(w);
        let store = self.db.store.object_store().clone();
        let at = self.layer.data_prefix().clone().join(name.as_str());
        fail::fail_point!("moraine::compact::stream_before_upload");
        let up = upload(&store, &at, &tmp.0, size);
        let sidecars = self.finish_inline(builds, &name, rows, size);
        let merges = self.finish_merged(merged, files, &name, rows, size);
        let (s, m) = if self.db.opts.io_concurrency > 1 {
            let (u, s, m) = futures::join!(up, sidecars, merges);
            u?;
            (s?, m?)
        } else {
            up.await?;
            (sidecars.await?, merges.await?)
        };
        drop(tmp);
        let clock = self.db.clock().clone();
        Ok((
            DataFile {
                path: name,
                size,
                stats,
                modification_time: clock.wall_now().timestamp_millis(),
                sorted_by: Some(snap.def.sort_columns()),
                added_in: 0,
                tags: Default::default(),
            },
            s,
            m,
        ))
    }

    /// Builds and uploads the merged sidecars of a finished output file from the inputs'
    /// sidecars and the row maps, then clears the maps.
    async fn finish_merged(
        &self,
        merged: &mut [Merged],
        files: &[Arc<DataFile>],
        name: &str,
        rows: u64,
        size: u64,
    ) -> Result<usize> {
        let mut n = 0;
        for m in merged.iter_mut() {
            let built = {
                let inputs: Vec<MergeInput<'_>> = m
                    .inputs
                    .iter()
                    .zip(&m.maps)
                    .map(|(bundle, rows)| MergeInput { bundle, rows })
                    .collect();
                let ctx = BuildCtx {
                    source_rows: rows,
                    budget_bytes: self.db.index.cfg.build_budget,
                    threads: 1,
                };
                let Some(builder) = m.kind.merge(&m.spec, &inputs, &ctx)? else {
                    return Err(Error::Internal(format!(
                        "index kind {} stopped merging",
                        m.kind.name()
                    )));
                };
                let mut b = BundleBuild::from_builder(
                    m.kind.as_ref(),
                    &m.spec,
                    builder,
                    rows,
                    size,
                    std::io::Cursor::new(Vec::new()),
                )?;
                b.set_source(rows, size);
                b.finish()?
            };
            for (map, f) in m.maps.iter_mut().zip(files) {
                *map = RowMap {
                    new: vec![None; f.rows().unwrap_or(0) as usize],
                };
            }
            let (w, len, _) = built;
            let location = sidecar_location(&self.idx_prefix(), name, &m.spec.name, &m.hash);
            let r = self
                .db
                .store
                .object_store()
                .put(&location, object_store::PutPayload::from(w.into_inner()))
                .await?;
            self.db
                .index
                .table(self.idx_prefix().as_ref())
                .discovery
                .add_own(
                    name,
                    &m.spec.name,
                    Listed {
                        location,
                        spec_hash: m.hash.clone(),
                        size: len,
                        etag: r.e_tag,
                    },
                );
            n += 1;
        }
        Ok(n)
    }
}

/// The rows of the chunk, from the inputs' current batches; the chunk is emptied.
fn take_chunk(inputs: &[Input], chunk: &mut Vec<(usize, usize)>) -> Result<Option<RecordBatch>> {
    if chunk.is_empty() {
        return Ok(None);
    }
    // The chunk refers only to inputs' current batches.
    let mut slot: HashMap<usize, usize> = HashMap::new();
    let mut used: Vec<&RecordBatch> = Vec::new();
    let mut pairs = Vec::with_capacity(chunk.len());
    for (i, r) in chunk.iter() {
        let s = match slot.get(i) {
            Some(s) => *s,
            None => {
                let b = inputs[*i].batch.as_ref().ok_or_else(|| {
                    Error::Internal("streaming compaction lost an input batch".into())
                })?;
                used.push(b);
                slot.insert(*i, used.len() - 1);
                used.len() - 1
            }
        };
        pairs.push((s, *r));
    }
    let rows = interleave_record_batch(&used, &pairs)?;
    chunk.clear();
    Ok(Some(rows))
}

/// Moves `inp` to its next non-empty batch (or the end) and encodes its sort keys.
async fn load(inp: &mut Input, conv: Option<&RowConverter>, idx: &[usize]) -> Result<()> {
    if let Some(b) = inp.batch.take() {
        inp.base += b.num_rows() as u64;
    }
    inp.keys = None;
    inp.pos = 0;
    while let Some(b) = inp.stream.next().await {
        let b = b?;
        if b.num_rows() == 0 {
            continue;
        }
        if let Some(c) = conv {
            let cols: Vec<_> = idx.iter().map(|i| b.column(*i).clone()).collect();
            inp.keys = Some(c.convert_columns(&cols)?);
        }
        inp.batch = Some(b);
        return Ok(());
    }
    Ok(())
}
