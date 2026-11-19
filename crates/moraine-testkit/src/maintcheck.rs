//! The checks of the maintenance simulations (`tests/maintenance_sim.rs`, scenario 5 in
//! `tests/crash_maintenance.rs`): a keyed table whose clients own their keys, read back from the
//! store's full history, deleted objects included, without moraine. See the simulation's
//! documentation for what is checked.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::Int64Type;
use chrono::{DateTime, Utc};
use moraine::{Error, MaintainOptions, Table};

use crate::datagen;
use crate::rng::SimRng;
use crate::sim::SimEnv;
use crate::table_log::{read_log_all, read_parquet_archived};

pub const ROOT: &str = "db";
pub const TABLE: &str = "t";
pub const GRACE: Duration = Duration::from_secs(600);

/// A stored row: key, writing operation, its index in the client's sequence, group.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Row {
    pub k: String,
    pub op: String,
    pub j: i64,
    pub g: Option<i64>,
}

pub fn to_rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut out = Vec::new();
    for b in batches {
        let k = b.column_by_name("k").expect("k").as_string::<i32>();
        let op = b.column_by_name("op").expect("op").as_string::<i32>();
        let i = b
            .column_by_name("i")
            .expect("i")
            .as_primitive::<Int64Type>();
        let g = b
            .column_by_name("g")
            .expect("g")
            .as_primitive::<Int64Type>();
        for r in 0..b.num_rows() {
            out.push(Row {
                k: k.value(r).to_string(),
                op: op.value(r).to_string(),
                j: i.value(r),
                g: (!g.is_null(r)).then(|| g.value(r)),
            });
        }
    }
    out.sort();
    out
}

pub fn batch(rows: &[Row]) -> RecordBatch {
    RecordBatch::try_new(
        datagen::row_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.k.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.op.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.j))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.g))),
        ],
    )
    .expect("batch")
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Acked(u64),
    Failed,
    Unknown,
}

#[derive(Clone, Debug)]
pub struct Write {
    pub key: String,
    pub j: i64,
    pub delete: bool,
    pub outcome: Outcome,
}

#[derive(Default)]
pub struct Record {
    /// op id -> write.
    pub writes: BTreeMap<String, Write>,
    /// (who, version, rows) of every read; pinned reads too.
    pub reads: Vec<(String, u64, Vec<Row>)>,
    pub maintenance_passes: u64,
    pub maintenance_errors: u64,
    pub expired: u64,
    pub restarted_reads: u64,
    pub pins: u64,
}

pub struct Done(pub Arc<std::sync::atomic::AtomicUsize>);

impl Drop for Done {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

pub fn record_write(
    rec: &Mutex<Record>,
    op: &str,
    key: &str,
    j: usize,
    delete: bool,
    r: moraine::Result<moraine::Committed>,
) {
    let outcome = match r {
        Ok(c) => Outcome::Acked(c.version),
        Err(Error::OutcomeUnknown { .. }) => Outcome::Unknown,
        Err(_) => Outcome::Failed,
    };
    rec.lock().unwrap().writes.insert(
        op.to_string(),
        Write {
            key: key.to_string(),
            j: j as i64,
            delete,
            outcome,
        },
    );
}

pub async fn pass(t: &Table, o: &MaintainOptions, rec: &Mutex<Record>) {
    let r = t.maintain(o).await;
    let mut g = rec.lock().unwrap();
    g.maintenance_passes += 1;
    if r.is_err() {
        g.maintenance_errors += 1;
    }
}

pub async fn read(t: &Table, who: &str, rec: &Mutex<Record>) {
    let Ok(snap) = t.snapshot().await else { return };
    if let Ok(b) = snap.scan().collect().await {
        rec.lock()
            .unwrap()
            .reads
            .push((who.to_string(), snap.version(), to_rows(&b)));
    }
}

pub async fn pin_or_unpin(t: &Table, c: usize, rng: &mut SimRng, rec: &Mutex<Record>) {
    let name = format!("c{c}");
    if rng.chance(0.5)
        && let Ok(v) = t.refresh().await
        && t.pin(&name, v).await.is_ok()
    {
        rec.lock().unwrap().pins += 1;
    } else if t.pins().is_ok_and(|p| p.contains_key(&name)) {
        // Read the pinned version, then maybe drop the pin.
        if let Ok(s) = t.snapshot_at_pin(&name).await
            && let Ok(b) = s.scan().collect().await
        {
            rec.lock()
                .unwrap()
                .reads
                .push((format!("pin {name}"), s.version(), to_rows(&b)));
        }
        if rng.chance(0.5) {
            let _ = t.unpin(&name).await;
        }
    }
}

#[derive(Default)]
pub struct Stats {
    pub versions: u64,
    pub compactions: u64,
    pub checkpoints: u64,
    pub deleted_files: u64,
    pub deleted_sidecars: u64,
    pub deleted_entries: u64,
}

/// From when to when (exclusive; `None`: still) something was live, in server time.
pub type Lifetime = (DateTime<Utc>, Option<DateTime<Utc>>);

pub fn check(env: &SimEnv, rec: &Record, final_version: u64) -> Result<Stats, String> {
    let root = format!("{ROOT}/{TABLE}/data");
    let entries = read_log_all(&env.store, &root);
    let archive = env.store.archive();
    let grace = chrono::Duration::from_std(GRACE).unwrap();
    let mut stats = Stats {
        versions: entries.len() as u64,
        ..Default::default()
    };
    // Replay: the live files, their rows, and the rows of every version.
    let mut live: BTreeMap<String, Vec<Row>> = BTreeMap::new();
    let mut by_version: BTreeMap<u64, Vec<Row>> = BTreeMap::new();
    let mut files_at: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    // data file -> (added at, removed at), server times of the entries.
    let mut lifetime: BTreeMap<String, Lifetime> = BTreeMap::new();
    // Pins: (name, version) -> (pinned at, unpinned at).
    let mut pins: BTreeMap<(String, u64), Lifetime> = BTreeMap::new();
    let mut current_pins: BTreeMap<String, u64> = BTreeMap::new();
    for e in &entries {
        let before: Vec<Row> = live.values().flatten().cloned().collect();
        for p in &e.removes {
            if live.remove(p).is_none() {
                return Err(format!(
                    "version {} removes {p}, which is not live",
                    e.version
                ));
            }
            if let Some(l) = lifetime.get_mut(p) {
                l.1 = Some(e.committed_at);
            }
        }
        for p in &e.adds {
            let rows = to_rows(&read_parquet_archived(&env.store, &format!("{root}/{p}")));
            live.insert(p.clone(), rows);
            lifetime.insert(p.clone(), (e.committed_at, None));
        }
        let mut rows: Vec<Row> = live.values().flatten().cloned().collect();
        rows.sort();
        let mut keys = BTreeSet::new();
        for r in &rows {
            if !keys.insert(r.k.clone()) {
                return Err(format!(
                    "I4: version {} holds key {:?} twice",
                    e.version, r.k
                ));
            }
        }
        if !e.data_change && (!e.adds.is_empty() || !e.removes.is_empty()) {
            stats.compactions += 1;
            let mut b = before.clone();
            b.sort();
            if b != rows {
                return Err(format!(
                    "I10: compaction at version {} changed rows",
                    e.version
                ));
            }
        }
        if let Some(cfg) = &e.configuration {
            let now: BTreeMap<String, u64> = cfg
                .get("moraine.pins")
                .map(|v| serde_json::from_str(v).expect("pins property"))
                .unwrap_or_default();
            for (n, v) in &current_pins {
                if now.get(n) != Some(v)
                    && let Some(p) = pins.get_mut(&(n.clone(), *v))
                    && p.1.is_none()
                {
                    p.1 = Some(e.committed_at);
                }
            }
            for (n, v) in &now {
                if current_pins.get(n) != Some(v) {
                    pins.insert((n.clone(), *v), (e.committed_at, None));
                }
            }
            current_pins = now;
        }
        files_at.insert(e.version, live.keys().cloned().collect());
        by_version.insert(e.version, rows);
    }
    // Writes: each key changes only by its owner's operations, in order.
    let mut first_seen: BTreeMap<String, u64> = BTreeMap::new();
    for (v, rows) in &by_version {
        for r in rows {
            first_seen.entry(r.op.clone()).or_insert(*v);
        }
    }
    for (op, w) in &rec.writes {
        if w.outcome == Outcome::Failed && !w.delete && first_seen.contains_key(op) {
            return Err(format!("failed upsert {op} is visible"));
        }
        let Outcome::Acked(v) = w.outcome else {
            continue;
        };
        let Some(rows) = by_version.get(&v) else {
            return Err(format!(
                "{op} acknowledged at version {v}, which does not exist"
            ));
        };
        let at = rows.iter().find(|r| r.k == w.key);
        if w.delete {
            if let Some(r) = at {
                return Err(format!("acknowledged delete {op} at {v} left {r:?}"));
            }
            // Scenario 3: no later version brings the key back with an older value.
            for (later, rows) in by_version.range(v..) {
                if let Some(r) = rows.iter().find(|r| r.k == w.key && r.j < w.j) {
                    return Err(format!(
                        "deleted key {:?} resurrected at version {later} with {r:?} after {op}",
                        w.key
                    ));
                }
            }
        } else if at.map(|r| r.op.as_str()) != Some(op.as_str()) {
            return Err(format!(
                "acknowledged upsert {op} is not at its version {v}: {at:?}"
            ));
        }
    }
    // Reads (I3).
    for (who, v, rows) in &rec.reads {
        match by_version.get(v) {
            Some(want) if want == rows => {}
            Some(want) => {
                return Err(format!(
                    "{who} read version {v} as {rows:?}, it holds {want:?}"
                ));
            }
            None => return Err(format!("{who} read version {v}, which does not exist")),
        }
    }
    // Scenario 13: nothing a recent snapshot or a pin needs was deleted.
    let pinned_files = |path: &str, at: DateTime<Utc>| -> Option<(String, u64)> {
        pins.iter()
            .find(|((_, v), (from, to))| {
                *from <= at
                    && to.is_none_or(|t| at < t)
                    && files_at.get(v).is_some_and(|f| f.contains(path))
            })
            .map(|(k, _)| k.clone())
    };
    let checkpoints: Vec<(u64, DateTime<Utc>)> = archive
        .iter()
        .filter_map(|(p, v)| {
            let name = p.strip_prefix(&format!("{root}/_delta_log/"))?;
            let stem = name.strip_suffix(".checkpoint.parquet")?;
            Some((stem.parse().ok()?, v[0].created))
        })
        .collect();
    stats.checkpoints = checkpoints.len() as u64;
    let data_prefix = format!("{root}/");
    let idx_prefix = format!("{ROOT}/{TABLE}/idx/");
    for (path, versions) in &archive {
        let Some(a) = versions.last() else { continue };
        let (true, Some(gone)) = (a.deleted, a.gone) else {
            continue;
        };
        let created = versions[0].created;
        if let Some(name) = path.strip_prefix(&format!("{root}/_delta_log/")) {
            if let Some(stem) = name.strip_suffix(".json") {
                stats.deleted_entries += 1;
                let v: u64 = stem.parse().expect("entry name");
                if gone < created + grace {
                    return Err(format!(
                        "log entry {v} deleted before the grace period passed"
                    ));
                }
                if !checkpoints.iter().any(|(c, at)| *c > v && *at <= gone) {
                    return Err(format!("log entry {v} deleted without a later checkpoint"));
                }
            }
            continue;
        }
        let (data_file, sidecar) = if let Some(rel) = path.strip_prefix(&idx_prefix) {
            let Some((file, _)) = rel.rsplit_once('/') else {
                continue;
            };
            (file.to_string(), true)
        } else if let Some(rel) = path.strip_prefix(&data_prefix)
            && rel.ends_with(".parquet")
            && !rel.starts_with('_')
        {
            (rel.to_string(), false)
        } else {
            continue;
        };
        if sidecar {
            stats.deleted_sidecars += 1;
        } else {
            stats.deleted_files += 1;
        }
        let what = if sidecar { "sidecar" } else { "data file" };
        match lifetime.get(&data_file) {
            Some((added, None)) => {
                return Err(format!(
                    "{what} {path} deleted while its file is live (written {created}, \
                     committed {added}, deleted {gone})"
                ));
            }
            Some((_, Some(removed))) if gone < *removed + grace => {
                return Err(format!(
                    "{what} {path} deleted {}s after its file was removed, inside the grace period",
                    (gone - *removed).num_seconds()
                ));
            }
            // A file no log entry ever added never committed: its writer deletes it as soon as
            // the write is over (D65), and vacuum after the grace period.
            _ => {}
        }
        if let Some(pin) = pinned_files(&data_file, gone) {
            return Err(format!("{what} {path} deleted while pin {pin:?} holds it"));
        }
    }
    // Scenario 14: after the quiet passes, exactly the live files remain.
    let live_now = files_at.get(&final_version).cloned().unwrap_or_default();
    let current = env.store.snapshot();
    let stored: BTreeSet<String> = current
        .keys()
        .filter_map(|p| p.strip_prefix(&data_prefix))
        .filter(|r| r.ends_with(".parquet") && !r.starts_with('_'))
        .map(str::to_string)
        .collect();
    if stored != live_now {
        let extra: Vec<_> = stored.difference(&live_now).collect();
        let missing: Vec<_> = live_now.difference(&stored).collect();
        return Err(format!(
            "after two quiet passes data/ holds {} extra files ({extra:?}) and misses {missing:?}",
            extra.len()
        ));
    }
    for p in current.keys() {
        if let Some(rel) = p.strip_prefix(&idx_prefix) {
            let file = rel.rsplit_once('/').map(|(f, _)| f).unwrap_or("");
            if !live_now.contains(file) {
                return Err(format!(
                    "sidecar {p} of a dead file survived the quiet passes"
                ));
            }
        }
        if p.starts_with(&format!("{ROOT}/{TABLE}/meta/inbox/")) {
            return Err(format!("inbox object {p} survived"));
        }
    }
    Ok(stats)
}
