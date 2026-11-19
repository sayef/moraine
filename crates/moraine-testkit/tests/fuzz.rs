//! Corruption fuzz tests (D29; case X3, T13's parsers on every commit): every parser that
//! reads bytes from the store gets mutated input, deterministically from a seed, on stable Rust
//! and in bounded time. Nothing may panic, hang, or allocate without bound; a corrupt object is
//! an error, or (for a mutation that keeps the input valid) an answer.
//!
//! - `log_entries_*`: log entries straight into the parser and the snapshot built from them.
//! - `table_properties_*` and `file_stats_*`: the JSON inside the metadata and add actions.
//! - `stored_objects_*`: one log entry or data file corrupted in the store, then the public API
//!   opens, scans, looks up and appends.
//!
//! More iterations: `MORAINE_FUZZ_ITERS=<n>` (per test); another seed: `MORAINE_FUZZ_SEED=<n>`.
//! A failure prints the iteration's seed, which replays it with `MORAINE_FUZZ_ITERS=1`, and
//! `MORAINE_FUZZ_VERBOSE=1` shows panic messages, including those moraine turned into errors.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::datatypes::{DataType, Field};
use futures::{FutureExt, TryStreamExt};
use moraine::expr::{ScalarValue, col, lit};
use moraine::{Consistency, Database, DuplicatePolicy, TableOptions, fuzzing};
use moraine_testkit::fuzz::{mutate_bytes, mutate_json, mutate_json_lines};
use moraine_testkit::{ClientClock, SimClock, SimRng, datagen};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde_json::Value;

/// Counts live heap bytes and their peak, so a test can bound what one input allocates.
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: forwards to the system allocator unchanged; the counters are only statistics.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: same contract as the caller's.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let now = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: same contract as the caller's.
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Peak heap growth allowed for one input: far above what the corpus needs (kilobytes), far
/// below what trusting a corrupt length field would ask for.
const PEAK_LIMIT: usize = 256 << 20;

/// The tests share the allocation counters, so they run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn iterations(default: u64) -> u64 {
    std::env::var("MORAINE_FUZZ_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn base_seed() -> u64 {
    std::env::var("MORAINE_FUZZ_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0x5EED)
}

/// Runs `f` once per iteration with its own generator; fails on a panic or a peak above
/// [`PEAK_LIMIT`], naming the seed. Returns how many iterations returned `true`.
fn each_iteration(name: &str, n: u64, mut f: impl FnMut(&mut SimRng) -> bool) -> u64 {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // moraine turns panics inside the Parquet decoder into errors (corrupt files reach some);
    // keep their messages out of the output unless asked for.
    let verbose = std::env::var_os("MORAINE_FUZZ_VERBOSE").is_some();
    let mut hook = (!verbose).then(|| {
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        hook
    });
    let mut ok = 0;
    for i in 0..n {
        let seed = base_seed().wrapping_add(i);
        let mut rng = SimRng::new(seed);
        let before = LIVE.load(Ordering::Relaxed);
        PEAK.store(before, Ordering::Relaxed);
        let r = std::panic::catch_unwind(AssertUnwindSafe(|| f(&mut rng)));
        let grew = PEAK.load(Ordering::Relaxed).saturating_sub(before);
        let failure = match r {
            Ok(_) if grew >= PEAK_LIMIT => Some(format!("one input allocated {grew} bytes")),
            Ok(accepted) => {
                ok += u64::from(accepted);
                None
            }
            Err(_) => Some("panicked (MORAINE_FUZZ_VERBOSE=1 shows where)".into()),
        };
        if let Some(why) = failure {
            if let Some(h) = hook.take() {
                std::panic::set_hook(h);
            }
            panic!("{name}: iteration {i} {why}; replay with MORAINE_FUZZ_SEED={seed}");
        }
    }
    if let Some(h) = hook {
        std::panic::set_hook(h);
    }
    println!("{name}: {n} inputs, {ok} accepted");
    ok
}

/// A small table's history: creation, appends, upserts, a delete, an update and an added
/// column, so the log holds every action kind moraine writes.
struct Corpus {
    store: Arc<InMemory>,
    entries: Vec<(Path, Vec<u8>)>,
    data: Vec<(Path, Vec<u8>)>,
}

/// A clock whose random names (writer ids, file names) follow `seed`, so the corpus and every
/// iteration replay byte for byte.
fn seeded_clock(seed: u64) -> Arc<ClientClock> {
    Arc::new(ClientClock::new(SimClock::new(), None, 0, seed))
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn corpus() -> Corpus {
    runtime().block_on(async {
        let store = Arc::new(InMemory::new());
        let db = Database::builder_for_store(store.clone(), "db")
            .clock(seeded_clock(1))
            .index_kind(Arc::new(moraine_testkit::toykind::SumKind))
            .open()
            .await
            .unwrap();
        let opts = TableOptions::default().primary_key(["k"]).cluster_by(["g"]);
        let t = db
            .create_table("t", datagen::row_schema(), opts)
            .await
            .unwrap();
        // The index list is a property the fuzzer reaches too (D33).
        t.create_index(moraine::IndexSpec::new("by_i", "test-sum", &["i"]))
            .await
            .unwrap();
        let mut rng = SimRng::new(11);
        for i in 0..3 {
            t.upsert_with(
                vec![datagen::rows(&mut rng, &format!("u{i}"), 20, Some(30), 4)],
                DuplicatePolicy::LastWins,
            )
            .await
            .unwrap();
        }
        t.delete(col("g").eq(lit(1i64))).await.unwrap();
        t.update(col("g").eq(lit(2i64)), vec![("i".into(), lit(-1i64))])
            .await
            .unwrap();
        t.add_column(Field::new("extra", DataType::Utf8, true))
            .await
            .unwrap();
        t.upsert_with(
            vec![datagen::rows(&mut rng, "after", 5, Some(30), 4)],
            DuplicatePolicy::LastWins,
        )
        .await
        .unwrap();
        let mut entries = Vec::new();
        let mut data = Vec::new();
        let metas: Vec<_> = store
            .list(Some(&Path::from("db/t")))
            .try_collect()
            .await
            .unwrap();
        for m in metas {
            let body = store.get(&m.location).await.unwrap().bytes().await.unwrap();
            let name = m.location.to_string();
            if name.ends_with(".json") && name.contains("_delta_log") {
                entries.push((m.location, body.to_vec()));
            } else if name.ends_with(".parquet") {
                data.push((m.location, body.to_vec()));
            }
        }
        entries.sort();
        data.sort();
        assert!(entries.len() >= 6 && !data.is_empty());
        Corpus {
            store,
            entries,
            data,
        }
    })
}

fn bodies(c: &Corpus) -> Vec<Vec<u8>> {
    c.entries.iter().map(|(_, b)| b.clone()).collect()
}

/// Failures replay from their seed only if the corpus is the same in every run.
#[test]
fn corpus_is_deterministic() {
    let (a, b) = (corpus(), corpus());
    assert_eq!(a.entries, b.entries);
    assert_eq!(a.data, b.data);
}

#[test]
fn log_entries_survive_corruption() {
    let c = corpus();
    let pristine = bodies(&c);
    let refs: Vec<&[u8]> = pristine.iter().map(Vec::as_slice).collect();
    assert!(fuzzing::log_entries(&refs).is_ok());
    let accepted = each_iteration("log_entries", iterations(4000), |rng| {
        let mut b = pristine.clone();
        for _ in 0..rng.range(1, 2) {
            let i = rng.below(b.len() as u64) as usize;
            b[i] = if rng.chance(0.8) {
                mutate_json_lines(rng, &b[i])
            } else {
                mutate_bytes(rng, &b[i])
            };
        }
        let refs: Vec<&[u8]> = b.iter().map(Vec::as_slice).collect();
        fuzzing::log_entries(&refs).is_ok()
    });
    assert!(accepted > 0, "no mutation kept the log valid");
}

/// The table properties of the corpus's second entry (the one creating the index).
fn properties(c: &Corpus) -> BTreeMap<String, String> {
    let text = String::from_utf8(c.entries[1].1.clone()).unwrap();
    let meta = text
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .find_map(|v| v.get("metaData").cloned())
        .unwrap();
    serde_json::from_value(meta["configuration"].clone()).unwrap()
}

#[test]
fn table_properties_survive_corruption() {
    let props = properties(&corpus());
    assert!(props["moraine.indexes"].contains("by_i"));
    assert!(fuzzing::table_properties(&props).is_ok());
    let keys: Vec<String> = props.keys().cloned().collect();
    let accepted = each_iteration("table_properties", iterations(20_000), |rng| {
        let mut p = props.clone();
        for _ in 0..rng.range(1, 3) {
            let k = &keys[rng.below(keys.len() as u64) as usize];
            let v = p.get(k).cloned().unwrap_or_default();
            let new = match rng.below(3) {
                0 => match serde_json::from_str::<Value>(&v) {
                    Ok(mut j) => {
                        mutate_json(rng, &mut j);
                        j.to_string()
                    }
                    Err(_) => String::from_utf8_lossy(&mutate_bytes(rng, v.as_bytes())).into(),
                },
                1 => String::from_utf8_lossy(&mutate_bytes(rng, v.as_bytes())).into(),
                _ => {
                    let mut j = Value::String(v);
                    mutate_json(rng, &mut j);
                    match j {
                        Value::String(s) => s,
                        other => other.to_string(),
                    }
                }
            };
            if rng.chance(0.1) {
                p.remove(k);
            } else {
                p.insert(k.clone(), new);
            }
        }
        fuzzing::table_properties(&p).is_ok()
    });
    assert!(accepted > 0);
}

#[test]
fn file_stats_survive_corruption() {
    let c = corpus();
    let mut stats = Vec::new();
    for (_, body) in &c.entries {
        for line in String::from_utf8_lossy(body).lines() {
            if let Ok(v) = serde_json::from_str::<Value>(line)
                && let Some(s) = v["add"]["stats"].as_str()
            {
                stats.push(s.to_string());
            }
        }
    }
    assert!(!stats.is_empty());
    let schema = datagen::row_schema();
    assert!(fuzzing::file_stats(&stats[0], &schema, true) > 0);
    let kept = each_iteration("file_stats", iterations(20_000), |rng| {
        let s = &stats[rng.below(stats.len() as u64) as usize];
        let text = match serde_json::from_str::<Value>(s) {
            Ok(mut v) if rng.chance(0.8) => {
                for _ in 0..rng.range(1, 3) {
                    mutate_json(rng, &mut v);
                }
                v.to_string()
            }
            _ => String::from_utf8_lossy(&mutate_bytes(rng, s.as_bytes())).into(),
        };
        fuzzing::file_stats(&text, &schema, rng.chance(0.5)) > 0
    });
    assert!(kept > 0);
}

/// One stored object corrupted, then the public API: open, scan, filter, key lookup, append.
/// Each step may fail; none may panic or hang.
#[test]
fn stored_objects_survive_corruption() {
    let c = corpus();
    let rt = runtime();
    let objects: Vec<(Path, Vec<u8>)> = c.entries.iter().chain(&c.data).cloned().collect();
    let accepted = each_iteration("stored_objects", iterations(300), |rng| {
        let store = Arc::new(InMemory::new());
        let target = rng.below(objects.len() as u64) as usize;
        let mutated = {
            let (path, body) = &objects[target];
            if path.as_ref().ends_with(".json") && rng.chance(0.8) {
                mutate_json_lines(rng, body)
            } else {
                mutate_bytes(rng, body)
            }
        };
        let mut rng = rng.fork(1);
        rt.block_on(async {
            for (i, (path, body)) in objects.iter().enumerate() {
                let b = if i == target { &mutated } else { body };
                store.put(path, PutPayload::from(b.clone())).await.unwrap();
            }
            let run = async {
                let db = Database::builder_for_store(store.clone(), "db")
                    .clock(seeded_clock(rng.next_u64()))
                    .consistency(Consistency::Strong)
                    .io_concurrency(1)
                    .open()
                    .await?;
                let t = db.open_table("t").await?;
                let mut fine = t.scan().count().await.is_ok();
                fine &= t
                    .scan()
                    .filter(col("g").eq(lit(3i64)))
                    .count()
                    .await
                    .is_ok();
                let key = ScalarValue::Utf8(Some(format!("k#{}", rng.below(30))));
                fine &= t.get(&[key]).await.is_ok();
                let rows = datagen::rows(&mut rng, "late", 3, Some(30), 4);
                fine &= t
                    .upsert_with(vec![rows], DuplicatePolicy::LastWins)
                    .await
                    .is_ok();
                Ok::<bool, moraine::Error>(fine)
            };
            let r = tokio::time::timeout(
                Duration::from_secs(30),
                AssertUnwindSafe(run).catch_unwind(),
            )
            .await
            .expect("an operation on a corrupt table hung");
            match r {
                Ok(Ok(fine)) => fine,
                Ok(Err(_)) => false,
                Err(p) => std::panic::resume_unwind(p),
            }
        })
    });
    // Some mutations change nothing that matters (an unread byte of a statistics string).
    assert!(accepted > 0);
    drop(c.store);
}

/// Sidecar bundles (T13 and V5 on stable Rust, D30): byte edits, and edits whose footer
/// checksum is recomputed so the header and section-table checks behind it are reached.
#[test]
fn bundles_survive_corruption() {
    let pristine = fuzzing::sample_bundle();
    assert_eq!(fuzzing::bundle(&pristine).ok(), Some(3));
    let accepted = each_iteration("bundles", iterations(20_000), |rng| {
        let mut b = pristine.clone();
        for _ in 0..rng.range(1, 3) {
            b = mutate_bytes(rng, &b);
        }
        if rng.chance(0.7) {
            fuzzing::bundle_reseal(&mut b);
        }
        fuzzing::bundle(&b).is_ok()
    });
    println!("bundles: {accepted} mutated bundles still valid");
}
