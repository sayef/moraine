//! The deterministic simulation scheduler.
//!
//! Logical clients run as tasks on a single-threaded runtime. Every store call (through
//! [`crate::ChaosStore`]) and every sleep (through [`crate::ClientClock`]) stops at a gate.
//! When every live client is stopped, the scheduler picks one gate from its seeded generator and
//! opens it; only that client runs until it reaches its next gate or finishes. Exactly one client
//! runs at a time, so the interleaving depends on the seed alone, and a seed replays exactly.
//!
//! Clients must not wait on anything but gates (no locks held across store calls, no real
//! timers), or the run cannot make progress; a step limit turns that into a failure instead of
//! a hang. A client may drop a future that waits at a gate (a simulated crash, a race of two
//! futures, a cancelled write): an unopened gate is then withdrawn and the client counts as
//! running again, and a gate that opened meanwhile is discarded without running anything.

use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::{Notify, oneshot};

use crate::clock::SimClock;
use crate::rng::SimRng;

/// What a client is waiting for at a gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Waiting {
    /// A store request: its kind (`get`, `put_create`, ...), its path, and whether it is a
    /// conditional write.
    Store {
        op: &'static str,
        path: String,
        conditional: bool,
    },
    /// A timer that fires at the given simulated time.
    Sleep { until: Duration },
}

struct Pending {
    /// Identifies the gate, so a gate dropped before it opens can find its entry.
    id: u64,
    client: usize,
    waiting: Waiting,
    /// Simulated time the call reached its gate.
    since: Duration,
    tx: oneshot::Sender<()>,
}

/// One scheduling decision, kept for printing a failing seed's history.
#[derive(Clone, Debug)]
pub struct Step {
    pub step: u64,
    pub at: Duration,
    pub client: usize,
    pub waiting: Waiting,
}

struct State {
    rng: SimRng,
    running: usize,
    pending: Vec<Pending>,
    trace: Vec<Step>,
    steps: u64,
    panics: Vec<(usize, String)>,
    next_gate: u64,
    /// Tasks clients spawned ([`Scheduler::spawn`]), aborted when the run ends.
    spawned: Vec<tokio::task::AbortHandle>,
}

/// How a run ended when it did not end normally.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunError {
    /// More scheduling steps than the limit: a livelock, or a client waiting outside a gate.
    StepLimit(u64),
    /// A client panicked.
    Panicked { client: usize, message: String },
}

/// A gate a client waits at; see [`Scheduler::gate`].
struct Parked<'a> {
    sched: &'a Scheduler,
    id: u64,
}

impl Drop for Parked<'_> {
    fn drop(&mut self) {
        let Ok(mut s) = self.sched.state.lock() else {
            return;
        };
        if let Some(i) = s.pending.iter().position(|p| p.id == self.id) {
            s.pending.remove(i);
            s.running += 1;
        }
    }
}

/// See the module documentation.
pub struct Scheduler {
    clock: SimClock,
    state: Mutex<State>,
    wake: Notify,
    max_steps: u64,
    /// Upper bound of the simulated latency added to each store call.
    max_latency: Duration,
    /// How long a store call may wait at its gate in simulated time while timers fire.
    max_request: Duration,
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("max_steps", &self.max_steps)
            .finish_non_exhaustive()
    }
}

impl Scheduler {
    pub fn new(seed: u64, clock: SimClock) -> Arc<Self> {
        Arc::new(Scheduler {
            clock,
            state: Mutex::new(State {
                rng: SimRng::new(seed),
                running: 0,
                pending: Vec::new(),
                trace: Vec::new(),
                steps: 0,
                panics: Vec::new(),
                next_gate: 0,
                spawned: Vec::new(),
            }),
            wake: Notify::new(),
            max_steps: 1_000_000,
            max_latency: Duration::from_millis(20),
            max_request: Duration::from_secs(30),
        })
    }

    pub fn clock(&self) -> &SimClock {
        &self.clock
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("scheduler lock")
    }

    /// Stops `client` until the scheduler picks this call.
    pub async fn gate(&self, client: usize, waiting: Waiting) {
        let (tx, rx) = oneshot::channel();
        let id = {
            let mut s = self.lock();
            s.next_gate += 1;
            let id = s.next_gate;
            s.pending.push(Pending {
                id,
                client,
                waiting,
                since: self.clock.now(),
                tx,
            });
            s.running -= 1;
            if s.running == 0 {
                self.wake.notify_one();
            }
            id
        };
        // A client polled while it waits here (a wake-up from another task) may drop this
        // future and go on, as a cancelled write does (D56): it is then running again, so the
        // unopened gate is withdrawn and counted back.
        let parked = Parked { sched: self, id };
        // The sender is only dropped when the run is torn down; then the client stops here.
        if rx.await.is_err() {
            futures::future::pending::<()>().await;
        }
        drop(parked);
    }

    /// Sleeps until simulated time `until`.
    pub async fn sleep_until(&self, client: usize, until: Duration) {
        self.gate(client, Waiting::Sleep { until }).await
    }

    /// Whether a client other than `client` is waiting to send a conditional write to `path`
    /// (two overlapping conditional writes; S3 may answer one of them with 409).
    pub fn overlapping_conditional(&self, client: usize, path: &str) -> bool {
        self.lock().pending.iter().any(|p| {
            p.client != client
                && matches!(&p.waiting, Waiting::Store { path: q, conditional: true, .. } if q == path)
        })
    }

    /// A random number from the scheduler's generator (for decisions that belong to the run as
    /// a whole, such as faults).
    pub fn random_u64(&self) -> u64 {
        self.lock().rng.next_u64()
    }

    /// Whether `client` is stopped at a gate that has not opened yet. A test that drops a
    /// client's future to model a crash drops it only when this is false, so the count of
    /// running clients stays right.
    pub fn is_waiting(&self, client: usize) -> bool {
        self.lock()
            .pending
            .iter()
            .any(|p| p.client == client && !p.tx.is_closed())
    }

    /// The last `n` scheduling decisions.
    pub fn trace_tail(&self, n: usize) -> Vec<Step> {
        let s = self.lock();
        s.trace[s.trace.len().saturating_sub(n)..].to_vec()
    }

    pub fn steps(&self) -> u64 {
        self.lock().steps
    }

    /// Runs `clients` to completion. Must be called inside a current-thread runtime (or a
    /// `LocalSet`); the clients are spawned on it.
    pub async fn run<F>(self: &Arc<Self>, clients: Vec<F>) -> Result<(), RunError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let n = clients.len();
        self.lock().running = n;
        let mut handles = Vec::with_capacity(n);
        for (i, c) in clients.into_iter().enumerate() {
            let me = self.clone();
            handles.push(tokio::spawn(async move {
                let r = AssertUnwindSafe(c).catch_unwind().await;
                let mut s = me.lock();
                if let Err(p) = r {
                    let msg = p
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_else(|| "panic".into());
                    s.panics.push((i, msg));
                }
                s.running -= 1;
                if s.running == 0 {
                    me.wake.notify_one();
                }
            }));
        }
        let result = self.drive().await;
        // Tear down: dropping the senders parks any client still at a gate forever; abort them.
        let spawned = {
            let mut s = self.lock();
            s.pending.clear();
            std::mem::take(&mut s.spawned)
        };
        for h in handles {
            h.abort();
        }
        for h in spawned {
            h.abort();
        }
        result
    }

    /// Runs `fut` as a task of `client` (a table handle's commit task, D64): it counts as
    /// running from now until it finishes, so the scheduler waits for it like for any client,
    /// and a panic in it fails the run.
    pub fn spawn(self: &Arc<Self>, client: usize, fut: futures::future::BoxFuture<'static, ()>) {
        self.lock().running += 1;
        let me = self.clone();
        let h = tokio::spawn(async move {
            let r = AssertUnwindSafe(fut).catch_unwind().await;
            let mut s = me.lock();
            if let Err(p) = r {
                let msg = p
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_else(|| "panic".into());
                s.panics.push((client, msg));
            }
            s.running -= 1;
            if s.running == 0 {
                me.wake.notify_one();
            }
        });
        self.lock().spawned.push(h.abort_handle());
    }

    async fn drive(&self) -> Result<(), RunError> {
        loop {
            // Wait until no client is running.
            loop {
                let notified = self.wake.notified();
                {
                    let s = self.lock();
                    if let Some((client, message)) = s.panics.first().cloned() {
                        return Err(RunError::Panicked { client, message });
                    }
                    if s.running == 0 {
                        break;
                    }
                }
                notified.await;
            }
            let mut s = self.lock();
            if let Some((client, message)) = s.panics.first().cloned() {
                return Err(RunError::Panicked { client, message });
            }
            // Gates whose waiting future was dropped: nothing runs when they open.
            s.pending.retain(|p| !p.tx.is_closed());
            if s.pending.is_empty() {
                return Ok(());
            }
            s.steps += 1;
            if s.steps > self.max_steps {
                return Err(RunError::StepLimit(self.max_steps));
            }
            let pick = self.pick(&mut s);
            let p = s.pending.swap_remove(pick);
            let now = match p.waiting {
                Waiting::Sleep { until } => {
                    self.clock.advance_to(until);
                    self.clock.now()
                }
                Waiting::Store { .. } => {
                    let lat = s.rng.below(self.max_latency.as_micros() as u64 + 1);
                    self.clock.advance(Duration::from_micros(lat));
                    self.clock.now()
                }
            };
            let step = s.steps;
            s.trace.push(Step {
                step,
                at: now,
                client: p.client,
                waiting: p.waiting.clone(),
            });
            s.running += 1;
            drop(s);
            let _ = p.tx.send(());
        }
    }

    /// Candidates: every pending store call, plus the earliest timer (firing it moves time
    /// forward). Ordered by client so the choice does not depend on arrival order.
    fn pick(&self, s: &mut State) -> usize {
        let mut cands: Vec<usize> = Vec::new();
        let mut earliest: Option<(Duration, usize, usize)> = None;
        for (i, p) in s.pending.iter().enumerate() {
            match p.waiting {
                Waiting::Store { .. } => cands.push(i),
                Waiting::Sleep { until } => {
                    let key = (until, p.client, i);
                    if earliest.is_none_or(|e| (key.0, key.1) < (e.0, e.1)) {
                        earliest = Some(key);
                    }
                }
            }
        }
        // A store call waits at most `max_request` for its turn: a timer that would move time
        // further fires only when no call is pending, as a real request times out rather than
        // stay in flight for minutes.
        let oldest_call = s
            .pending
            .iter()
            .filter(|p| matches!(p.waiting, Waiting::Store { .. }))
            .map(|p| p.since)
            .min();
        if let Some((until, _, i)) = earliest
            && (cands.is_empty() || oldest_call.is_none_or(|o| until <= o + self.max_request))
        {
            cands.push(i);
        }
        cands.sort_by_key(|&i| (s.pending[i].client, i));
        let k = s.rng.below(cands.len() as u64) as usize;
        cands[k]
    }
}
