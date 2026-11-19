//! Simulated time: one [`SimClock`] for the server side, a [`ClientClock`] per client.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use futures::future::BoxFuture;
use moraine::clock::Clock;

use crate::rng::SimRng;
use crate::sched::Scheduler;

/// Simulated time shared by a [`crate::SimStore`] (its last-modified times) and every client's
/// monotonic clock. Only moves when told to.
#[derive(Clone, Debug)]
pub struct SimClock {
    now: Arc<Mutex<Duration>>,
}

/// Server time at simulated time zero.
pub fn sim_epoch() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .expect("valid date")
}

impl SimClock {
    pub fn new() -> Self {
        SimClock {
            now: Arc::new(Mutex::new(Duration::ZERO)),
        }
    }

    /// Simulated time since the start.
    pub fn now(&self) -> Duration {
        *self.now.lock().expect("clock lock")
    }

    /// The object store's wall clock.
    pub fn server_time(&self) -> DateTime<Utc> {
        sim_epoch() + chrono::Duration::from_std(self.now()).expect("in range")
    }

    pub fn advance(&self, d: Duration) {
        *self.now.lock().expect("clock lock") += d;
    }

    /// Moves to `t` if that is later than now.
    pub fn advance_to(&self, t: Duration) {
        let mut now = self.now.lock().expect("clock lock");
        if t > *now {
            *now = t;
        }
    }
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new()
    }
}

/// One client's view of simulated time: monotonic time is the shared [`SimClock`]; its wall
/// clock may be skewed (case G1); sleeping goes through the [`Scheduler`] when there is one,
/// and otherwise jumps the clock forward.
#[derive(Debug)]
pub struct ClientClock {
    sim: SimClock,
    sched: Option<Arc<Scheduler>>,
    client: usize,
    skew_ms: i64,
    rng: Mutex<SimRng>,
}

impl ClientClock {
    pub fn new(sim: SimClock, sched: Option<Arc<Scheduler>>, client: usize, seed: u64) -> Self {
        ClientClock {
            sim,
            sched,
            client,
            skew_ms: 0,
            rng: Mutex::new(SimRng::new(
                seed ^ (client as u64).wrapping_mul(0xA24B_AED4_963E_E407),
            )),
        }
    }

    /// A wall clock that is `skew_ms` milliseconds off server time.
    pub fn with_skew(mut self, skew_ms: i64) -> Self {
        self.skew_ms = skew_ms;
        self
    }

    /// The client's (possibly wrong) wall clock.
    pub fn wall_now(&self) -> DateTime<Utc> {
        self.sim.server_time() + chrono::Duration::milliseconds(self.skew_ms)
    }

    pub fn client(&self) -> usize {
        self.client
    }
}

impl Clock for ClientClock {
    fn now(&self) -> Duration {
        self.sim.now()
    }

    fn sleep(&self, d: Duration) -> BoxFuture<'static, ()> {
        match &self.sched {
            Some(s) => {
                let s = s.clone();
                let until = self.sim.now() + d;
                let client = self.client;
                Box::pin(async move { s.sleep_until(client, until).await })
            }
            None => {
                self.sim.advance(d);
                Box::pin(async {})
            }
        }
    }

    fn random_u64(&self) -> u64 {
        self.rng.lock().expect("rng lock").next_u64()
    }

    fn wall_now(&self) -> DateTime<Utc> {
        ClientClock::wall_now(self)
    }

    /// Under the scheduler the task counts as a running client until it finishes.
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        match &self.sched {
            Some(s) => s.spawn(self.client, fut),
            None => {
                tokio::spawn(fut);
            }
        }
    }

    /// Under the scheduler a task may only wait at a gate, so the event is polled once per
    /// simulated millisecond of sleep; without one it is awaited directly.
    fn wait_for<'a>(&self, fut: BoxFuture<'a, ()>) -> BoxFuture<'a, ()> {
        if self.sched.is_none() {
            return fut;
        }
        let sleeper = ClientClock {
            sim: self.sim.clone(),
            sched: self.sched.clone(),
            client: self.client,
            skew_ms: self.skew_ms,
            rng: Mutex::new(SimRng::new(0)),
        };
        Box::pin(async move {
            let mut fut = fut;
            loop {
                if futures::poll!(&mut fut).is_ready() {
                    return;
                }
                sleeper.sleep(Duration::from_millis(1)).await;
            }
        })
    }
}
