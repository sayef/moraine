//! Running scenarios under many seeds.
//!
//! A scenario gets a fresh [`SimEnv`] per seed: a simulated clock, an in-memory store, a
//! scheduler, and a generator for its own choices. It returns `Err` with a description when an
//! invariant fails. [`run_seeds`] stops at the first failing seed and reports it with the tail of
//! its schedule; running that seed alone ([`seeds_from_env`] with `MORAINE_SIM_SEED`) replays
//! the same run.

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

use object_store::ObjectStore;
use {moraine::store::RetryPolicy, moraine::testing::Store};

use crate::chaos::{ChaosStore, FaultCounts, FaultRules};
use crate::clock::{ClientClock, SimClock};
use crate::rng::SimRng;
use crate::sched::{RunError, Scheduler, Step};
use crate::sim_store::SimStore;
use moraine::store::StoreMetrics;

/// Everything one seed's run shares.
pub struct SimEnv {
    pub seed: u64,
    pub clock: SimClock,
    pub sched: Arc<Scheduler>,
    pub store: SimStore,
    /// For the scenario's own choices (client count, operation mix, fault rules).
    pub rng: SimRng,
    /// Requests and faults across every client's [`ChaosStore`].
    pub requests: Arc<StoreMetrics>,
    pub faults: Arc<FaultCounts>,
}

impl SimEnv {
    pub fn new(seed: u64) -> Self {
        let clock = SimClock::new();
        let mut rng = SimRng::new(seed);
        let sched = Scheduler::new(rng.next_u64(), clock.clone());
        SimEnv {
            seed,
            store: SimStore::new(clock.clone()),
            clock,
            sched,
            rng,
            requests: Arc::default(),
            faults: Arc::default(),
        }
    }

    /// Client `client`'s clock: simulated time, sleeps through the scheduler.
    pub fn client_clock(&self, client: usize) -> Arc<ClientClock> {
        Arc::new(ClientClock::new(
            self.clock.clone(),
            Some(self.sched.clone()),
            client,
            self.seed,
        ))
    }

    /// Client `client`'s raw store: the shared [`SimStore`] behind a [`ChaosStore`] with
    /// `rules`, gated by the scheduler.
    pub fn chaos_store(&self, client: usize, rules: FaultRules) -> Arc<dyn ObjectStore> {
        let seed = self.seed ^ (client as u64 + 1).wrapping_mul(0xD1B5_4A32_D192_ED03);
        Arc::new(
            ChaosStore::new(Arc::new(self.store.clone()), rules, seed)
                .with_scheduler(self.sched.clone(), client)
                .with_counters(self.requests.clone(), self.faults.clone()),
        )
    }

    /// Client `client`'s store as moraine uses it: retries and metrics over
    /// [`SimEnv::chaos_store`], on the client's clock.
    pub fn client_store(&self, client: usize, rules: FaultRules, policy: RetryPolicy) -> Store {
        Store::new(
            self.chaos_store(client, rules),
            self.client_clock(client),
            policy,
        )
    }

    /// Runs `clients` under the scheduler.
    pub async fn run<F>(&self, clients: Vec<F>) -> Result<(), RunError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.sched.run(clients).await
    }
}

/// A seed whose run broke an invariant (or panicked, or did not finish).
#[derive(Debug)]
pub struct SeedFailure {
    pub seed: u64,
    pub reason: String,
    /// The last scheduling decisions before the failure.
    pub trace: Vec<Step>,
}

impl std::fmt::Display for SeedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "seed {} failed: {}", self.seed, self.reason)?;
        writeln!(f, "replay with MORAINE_SIM_SEED={}", self.seed)?;
        writeln!(f, "last scheduling steps:")?;
        for s in &self.trace {
            writeln!(
                f,
                "  #{:<5} t={:>10.3?} client {} {:?}",
                s.step, s.at, s.client, s.waiting
            )?;
        }
        Ok(())
    }
}

/// Runs one seed on a fresh single-threaded runtime.
pub fn run_seed<F, Fut>(seed: u64, scenario: F) -> Result<(), SeedFailure>
where
    F: FnOnce(SimEnv) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let env = SimEnv::new(seed);
    let sched = env.sched.clone();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    // A seed that hangs fails the run instead of stalling it: a watchdog thread ends the
    // process once the seed has run longer than `MORAINE_SIM_SEED_TIMEOUT_S` seconds of real
    // time (default 120), naming the seed to replay.
    let (done, watch) = std::sync::mpsc::channel::<()>();
    let limit = std::env::var("MORAINE_SIM_SEED_TIMEOUT_S")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(120);
    let watchdog = std::thread::spawn(move || {
        if let Err(std::sync::mpsc::RecvTimeoutError::Timeout) =
            watch.recv_timeout(std::time::Duration::from_secs(limit))
        {
            eprintln!(
                "seed {seed} still running after {limit} s: replay with MORAINE_SIM_SEED={seed}"
            );
            std::process::exit(101);
        }
    });
    let r = rt.block_on(scenario(env));
    let _ = done.send(());
    let _ = watchdog.join();
    r.map_err(|reason| SeedFailure {
        seed,
        reason,
        trace: sched.trace_tail(30),
    })
}

/// Runs `seeds` in order and returns the first failure.
pub fn run_seeds<F, Fut>(seeds: Range<u64>, scenario: F) -> Result<u64, SeedFailure>
where
    F: Fn(SimEnv) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let mut n = 0;
    for seed in seeds {
        run_seed(seed, &scenario)?;
        n += 1;
    }
    Ok(n)
}

/// The seeds to run: `MORAINE_SIM_SEED=<n>` runs that seed alone; otherwise
/// `MORAINE_SIM_SEEDS=<count>` (default `default_count`) seeds from `MORAINE_SIM_SEED_START`
/// (default 0).
pub fn seeds_from_env(default_count: u64) -> Range<u64> {
    let var = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<u64>().ok());
    if let Some(s) = var("MORAINE_SIM_SEED") {
        return s..s + 1;
    }
    let start = var("MORAINE_SIM_SEED_START").unwrap_or(0);
    start..start + var("MORAINE_SIM_SEEDS").unwrap_or(default_count)
}
