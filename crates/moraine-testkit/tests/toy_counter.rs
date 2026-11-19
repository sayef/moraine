//! The harness's first acceptance test: a toy client that keeps a counter in one object,
//! updated by compare-and-swap through moraine's store layer and commit retry loop, passes the
//! simulation under 10,000 seeds with faults injected, and deliberately broken versions fail
//! with a seed that replays.
//!
//! The counter object holds the ids of the increments applied, in order, so the object is its
//! own commit log: version `n` is the first `n` ids. Each increment is one logical write with a
//! unique id, exactly like a table commit with a transaction id, and the invariant checker
//! compares what clients were told with that log.
//!
//! Run more seeds with `MORAINE_SIM_SEEDS=100000`, or one seed with `MORAINE_SIM_SEED=<n>`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use moraine::Error;
use moraine::clock::Clock;
use moraine::testing::{AttemptError, LoopStats, commit_with_retries};
use moraine_testkit::check::{Outcome, Recorder};
use moraine_testkit::model::ModelOp;
use moraine_testkit::sim::{SimEnv, run_seed, run_seeds, seeds_from_env};
use moraine_testkit::{FaultRules, SimRng};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use {
    moraine::store::{Path, RetryPolicy},
    moraine::testing::{Class, classify},
};

const COUNTER: &str = "counter.json";

/// Seeds that failed at some point during development; they run on every test run.
const REGRESSION_SEEDS: &[u64] = &[];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Variant {
    /// Compare-and-swap; an ambiguous outcome is looked up before retrying.
    Correct,
    /// Broken: retries after an ambiguous outcome without looking (an append lands twice).
    NoLookup,
    /// Broken: plain overwrites instead of compare-and-swap (concurrent increments are lost).
    BlindOverwrite,
}

#[derive(Default, Debug)]
struct Totals {
    seeds: u64,
    clients: u64,
    ops: u64,
    acked: u64,
    failed: u64,
    unknown: u64,
    requests: u64,
    faults: u64,
    ambiguous_faults: u64,
    loop_stats: LoopStats,
}

/// Reads the counter: (version, ids, ETag). `None` when it does not exist yet.
async fn read(
    store: &dyn ObjectStore,
) -> Result<Option<(u64, Vec<String>, Option<String>)>, object_store::Error> {
    match store.get(&Path::from(COUNTER)).await {
        Ok(r) => {
            let e_tag = r.meta.e_tag.clone();
            let body = r.bytes().await?;
            let ids: Vec<String> = serde_json::from_slice(&body)
                .unwrap_or_else(|e| panic!("counter object is not valid JSON ({e}): {body:?}"));
            Ok(Some((ids.len() as u64, ids, e_tag)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e),
    }
}

fn read_error(e: object_store::Error) -> AttemptError {
    match classify(&e) {
        // A failed read changed nothing: try the attempt again.
        Class::Throttled | Class::Unavailable | Class::Ambiguous | Class::Corrupt => {
            AttemptError::Throttled
        }
        _ => AttemptError::Fatal(Error::from(e)),
    }
}

/// One attempt: read, append `op`, write back conditionally.
async fn attempt(
    store: Arc<dyn ObjectStore>,
    op: String,
    variant: Variant,
) -> Result<u64, AttemptError> {
    let current = read(store.as_ref()).await.map_err(read_error)?;
    let (mut ids, e_tag) = match current {
        Some((_, ids, e_tag)) => (ids, e_tag),
        None => (Vec::new(), None),
    };
    if variant == Variant::Correct {
        // Already applied by an earlier attempt whose outcome looked unknown.
        if let Some(i) = ids.iter().position(|x| *x == op) {
            return Ok(i as u64 + 1);
        }
    }
    let had_object = e_tag.is_some() || !ids.is_empty();
    ids.push(op);
    let mode = match (variant, had_object) {
        (Variant::BlindOverwrite, _) => PutMode::Overwrite,
        (_, false) => PutMode::Create,
        (_, true) => PutMode::Update(UpdateVersion {
            e_tag,
            version: None,
        }),
    };
    let body = PutPayload::from(serde_json::to_vec(&ids).expect("json"));
    let opts = PutOptions {
        mode,
        ..Default::default()
    };
    match store.put_opts(&Path::from(COUNTER), body, opts).await {
        Ok(_) => Ok(ids.len() as u64),
        Err(e) => Err(AttemptError::from_store(e)),
    }
}

/// Looks `op` up after an ambiguous outcome.
async fn resolve(
    store: Arc<dyn ObjectStore>,
    op: String,
    variant: Variant,
) -> moraine::Result<Option<u64>> {
    if variant == Variant::NoLookup {
        return Ok(None);
    }
    let current = read(store.as_ref()).await?;
    Ok(current.and_then(|(_, ids, _)| ids.iter().position(|x| *x == op).map(|i| i as u64 + 1)))
}

async fn increment(
    store: &Arc<dyn ObjectStore>,
    clock: &dyn Clock,
    op: &str,
    variant: Variant,
) -> (moraine::Result<u64>, LoopStats) {
    let (s1, s2, o1, o2) = (store.clone(), store.clone(), op.to_string(), op.to_string());
    commit_with_retries(
        &RetryPolicy::COMMIT,
        clock,
        op,
        move |_| attempt(s1.clone(), o1.clone(), variant).boxed(),
        move || resolve(s2.clone(), o2.clone(), variant).boxed(),
    )
    .await
}

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

async fn scenario(
    mut env: SimEnv,
    variant: Variant,
    totals: Arc<Mutex<Totals>>,
) -> Result<(), String> {
    let clients = env.rng.range(2, 5) as usize;
    let ops_per_client = env.rng.range(2, 6);
    let rules = FaultRules::random(&mut env.rng);
    let recorder = Recorder::<String>::default();
    let loop_totals = Arc::new(Mutex::new(LoopStats::default()));

    let mut futs: Vec<ClientFuture> = Vec::new();
    for c in 0..clients {
        let store = env.client_store(c, rules.clone(), RetryPolicy::REQUEST);
        let rec = recorder.clone();
        let lt = loop_totals.clone();
        futs.push(Box::pin(async move {
            let os = store.object_store().clone();
            let clock = store.clock().clone();
            for i in 0..ops_per_client {
                let op = format!("c{c}-{i}");
                rec.intend(c, &op, ModelOp::Append(vec![op.clone()]));
                let (r, stats) = increment(&os, clock.as_ref(), &op, variant).await;
                {
                    let mut t = lt.lock().unwrap();
                    t.attempts += stats.attempts;
                    t.conflicts += stats.conflicts;
                    t.throttled += stats.throttled;
                    t.ambiguous += stats.ambiguous;
                    t.resolved_found += stats.resolved_found;
                }
                let outcome = match r {
                    Ok(version) => Outcome::Acked { version },
                    Err(Error::OutcomeUnknown { .. }) => Outcome::Unknown,
                    Err(e) => Outcome::Failed(e.to_string()),
                };
                rec.outcome(c, &op, outcome);
                // A read after every write: checks I3 at that version and I5 for this client.
                if let Ok(current) = read(os.as_ref()).await {
                    let (version, ids) = current.map(|(v, ids, _)| (v, ids)).unwrap_or_default();
                    rec.read(c, version, ids);
                }
            }
        }));
    }
    env.run(futs)
        .await
        .map_err(|e| format!("run did not finish: {e:?}"))?;

    // The true log, read straight from the simulated store.
    let ids: Vec<String> = env
        .store
        .peek(COUNTER)
        .map(|b| serde_json::from_slice(&b).expect("json"))
        .unwrap_or_default();
    let log = ids
        .iter()
        .enumerate()
        .map(|(i, id)| (i as u64 + 1, id.clone(), ModelOp::Append(vec![id.clone()])))
        .collect();
    let history = recorder.history(log);

    let (acked, failed, unknown) = recorder.outcome_counts();
    {
        let mut t = totals.lock().unwrap();
        let lt = loop_totals.lock().unwrap();
        t.seeds += 1;
        t.clients += clients as u64;
        t.ops += clients as u64 * ops_per_client;
        t.acked += acked as u64;
        t.failed += failed as u64;
        t.unknown += unknown as u64;
        t.requests += env.requests.snapshot().requests();
        t.faults += env.faults.total();
        t.ambiguous_faults += env.faults.ambiguous();
        t.loop_stats.attempts += lt.attempts;
        t.loop_stats.conflicts += lt.conflicts;
        t.loop_stats.throttled += lt.throttled;
        t.loop_stats.ambiguous += lt.ambiguous;
        t.loop_stats.resolved_found += lt.resolved_found;
    }
    history.check(false).map_err(|v| v.to_string())
}

fn run(
    variant: Variant,
    seeds: std::ops::Range<u64>,
) -> (Result<u64, moraine_testkit::sim::SeedFailure>, Totals) {
    let totals = Arc::new(Mutex::new(Totals::default()));
    let t = totals.clone();
    let r = run_seeds(seeds, move |env| scenario(env, variant, t.clone()));
    let totals = std::mem::take(&mut *totals.lock().unwrap());
    (r, totals)
}

#[test]
fn toy_counter_passes_every_seed() {
    let seeds = seeds_from_env(10_000);
    let (r, t) = run(Variant::Correct, seeds.clone());
    if let Err(f) = r {
        panic!("{f}");
    }
    println!(
        "RESULT toy_counter seeds={}..{} ({} seeds) clients={} ops={} acked={} failed={} unknown={} \
         requests={} faults={} ambiguous_faults={} attempts={} conflicts={} throttled={} \
         ambiguous_outcomes={} resolved_as_committed={}",
        seeds.start,
        seeds.end,
        t.seeds,
        t.clients,
        t.ops,
        t.acked,
        t.failed,
        t.unknown,
        t.requests,
        t.faults,
        t.ambiguous_faults,
        t.loop_stats.attempts,
        t.loop_stats.conflicts,
        t.loop_stats.throttled,
        t.loop_stats.ambiguous,
        t.loop_stats.resolved_found,
    );
    assert_eq!(t.acked + t.failed + t.unknown, t.ops);
    // The faults must actually exercise the ambiguous path, or the run proves little.
    assert!(t.loop_stats.ambiguous > 0 && t.loop_stats.resolved_found > 0);
    assert!(t.loop_stats.conflicts > 0);
}

#[test]
fn regression_seeds() {
    for &seed in REGRESSION_SEEDS {
        let (r, _) = run(Variant::Correct, seed..seed + 1);
        if let Err(f) = r {
            panic!("{f}");
        }
    }
}

/// A broken client must be caught, and its failing seed must replay to the same failure.
fn broken_is_caught(variant: Variant, invariants: &[&str]) {
    let (r, _) = run(variant, 0..2_000);
    let failure = r.expect_err("the broken client passed every seed");
    println!("RESULT broken {variant:?}: {failure}");
    assert!(
        invariants.iter().any(|i| failure.reason.starts_with(i)),
        "expected one of {invariants:?}: {}",
        failure.reason
    );
    let totals = Arc::new(Mutex::new(Totals::default()));
    let again = run_seed(failure.seed, |env| scenario(env, variant, totals.clone()))
        .expect_err("replaying the failing seed passed");
    assert_eq!(again.reason, failure.reason, "replay differs");
    assert_eq!(
        format!("{:?}", again.trace),
        format!("{:?}", failure.trace),
        "replayed schedule differs"
    );
}

#[test]
fn broken_no_lookup_lands_twice() {
    broken_is_caught(Variant::NoLookup, &["I10"]);
}

#[test]
fn broken_blind_overwrite_loses_increments() {
    // A lost increment shows as an acknowledged write missing from the log (I1), or as a read
    // that no replay of the log produces (I3).
    broken_is_caught(Variant::BlindOverwrite, &["I1", "I3"]);
}

#[test]
fn rules_are_deterministic() {
    let a = FaultRules::random(&mut SimRng::new(3));
    let b = FaultRules::random(&mut SimRng::new(3));
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}
