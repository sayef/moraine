//! `ChaosStore`: fault injection driven by a seed and a rule list.
//!
//! Faults, each with its own probability per request:
//!
//! - **fail before**: the request does not take effect; the client sees a refused connection or
//!   a timeout (it cannot tell which case it is in from a timeout).
//! - **fail after**: the request takes effect and the response is lost (timeout or reset). This
//!   is the ambiguous outcome of case S1.
//! - **overlap 409**: a conditional write while another client's conditional write to the same
//!   path is in flight gets 409 `ConditionalRequestConflict`, as S3 does; whether it took effect
//!   is drawn at random, and the client must treat it as ambiguous.
//! - **throttle**: 503 SlowDown, not applied (case S3).
//! - **stall**: the request takes `stall_for` of simulated time (case S12).
//! - **truncate**: a read returns fewer bytes than announced (case S13).
//! - **cut listing**: a listing fails after part of its results (case S4).
//! - **conditional writes**: honoured, rejected (`NotImplemented`), or silently ignored and
//!   performed as plain overwrites (case S2).
//!
//! Every faulted error is a [`moraine::testing::StoreFault`] with the class a real client would
//! infer, so `moraine::testing::classify` sees exactly what it would see in production.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyMode, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result,
};
use {
    moraine::store::{MetricsSnapshot, Op, StoreMetrics},
    moraine::testing::{Class, StoreFault},
};

use crate::clock::SimClock;
use crate::rng::SimRng;
use crate::sched::{Scheduler, Waiting};

/// What the store does with conditional writes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Conditional {
    #[default]
    Honour,
    /// Refuse them with `NotImplemented`.
    Reject,
    /// Accept them and write unconditionally.
    Ignore,
}

/// Fault probabilities per request, and the other knobs. All zero by default.
#[derive(Clone, Debug, Default)]
pub struct FaultRules {
    pub fail_before: f64,
    pub fail_after: f64,
    pub overlap_409: f64,
    pub throttle: f64,
    pub stall: f64,
    pub stall_for: Duration,
    pub truncate: f64,
    pub cut_listing: f64,
    pub conditional: Conditional,
    /// Probability that a conditional write of a Delta log entry (`/_delta_log/`) takes effect
    /// and its response is lost, on top of `fail_after` (scenario 4 sets it to 1: every commit
    /// is ambiguous, while other conditional writes such as the capability probe still work).
    pub ambiguous_commits: f64,
    /// Probability that a conditional write of a Delta log entry takes effect and then fails
    /// with a plain `object_store::Error::Generic` whose message names no known failure class
    /// (an unrecognised response, as a store or proxy can return). Nothing says the write did
    /// not happen, so the client must look it up.
    pub generic_after_commits: f64,
}

impl FaultRules {
    /// Rules drawn from a seed, for simulation runs: each fault is off or on at a low rate.
    pub fn random(rng: &mut SimRng) -> FaultRules {
        let mut p = |max: f64| {
            if rng.chance(0.5) {
                0.0
            } else {
                (rng.below(1000) as f64 / 1000.0) * max
            }
        };
        FaultRules {
            fail_before: p(0.10),
            fail_after: p(0.10),
            overlap_409: p(0.5),
            throttle: p(0.10),
            stall: p(0.02),
            stall_for: Duration::from_secs(30),
            truncate: p(0.10),
            cut_listing: p(0.2),
            conditional: Conditional::Honour,
            ambiguous_commits: 0.0,
            generic_after_commits: p(0.05),
        }
    }
}

/// Counts of injected faults.
#[derive(Debug, Default)]
pub struct FaultCounts {
    pub fail_before: AtomicU64,
    pub fail_after: AtomicU64,
    pub overlap_409: AtomicU64,
    pub throttle: AtomicU64,
    pub stall: AtomicU64,
    pub truncate: AtomicU64,
    pub cut_listing: AtomicU64,
}

impl FaultCounts {
    pub fn total(&self) -> u64 {
        [
            &self.fail_before,
            &self.fail_after,
            &self.overlap_409,
            &self.throttle,
            &self.stall,
            &self.truncate,
            &self.cut_listing,
        ]
        .iter()
        .map(|c| c.load(Ordering::Relaxed))
        .sum()
    }

    /// Faults that may have taken effect while the client saw an error.
    pub fn ambiguous(&self) -> u64 {
        self.fail_after.load(Ordering::Relaxed) + self.overlap_409.load(Ordering::Relaxed)
    }
}

/// See the module documentation.
pub struct ChaosStore {
    inner: Arc<dyn ObjectStore>,
    rules: FaultRules,
    rng: Mutex<SimRng>,
    sched: Option<(Arc<Scheduler>, usize)>,
    clock: Option<SimClock>,
    requests: Arc<StoreMetrics>,
    faults: Arc<FaultCounts>,
}

impl fmt::Debug for ChaosStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChaosStore")
            .field("rules", &self.rules)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ChaosStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Chaos({})", self.inner)
    }
}

enum Fault {
    None,
    Before(Class, &'static str),
    After(&'static str),
    /// Takes effect, then fails with an unclassified `Generic` error.
    GenericAfter,
    Throttle,
}

impl ChaosStore {
    pub fn new(inner: Arc<dyn ObjectStore>, rules: FaultRules, seed: u64) -> Self {
        ChaosStore {
            inner,
            rules,
            rng: Mutex::new(SimRng::new(seed)),
            sched: None,
            clock: None,
            requests: Arc::default(),
            faults: Arc::default(),
        }
    }

    /// Puts every request of this store, as client `client`, through the scheduler's gates.
    pub fn with_scheduler(mut self, sched: Arc<Scheduler>, client: usize) -> Self {
        self.clock = Some(sched.clock().clone());
        self.sched = Some((sched, client));
        self
    }

    /// The clock that stalls advance (taken from the scheduler when there is one).
    pub fn with_clock(mut self, clock: SimClock) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Shares request and fault counters with other `ChaosStore`s.
    pub fn with_counters(mut self, requests: Arc<StoreMetrics>, faults: Arc<FaultCounts>) -> Self {
        self.requests = requests;
        self.faults = faults;
        self
    }

    pub fn requests(&self) -> MetricsSnapshot {
        self.requests.snapshot()
    }

    pub fn faults(&self) -> &Arc<FaultCounts> {
        &self.faults
    }

    fn chance(&self, p: f64) -> bool {
        p > 0.0 && self.rng.lock().expect("rng lock").chance(p)
    }

    /// Gate, count, and maybe stall.
    async fn enter(&self, op: Op, path: &Path, conditional: bool) {
        if let Some((s, client)) = &self.sched {
            s.gate(
                *client,
                Waiting::Store {
                    op: op.name(),
                    path: path.to_string(),
                    conditional,
                },
            )
            .await;
        }
        self.requests.record(op);
        if self.chance(self.rules.stall) {
            self.faults.stall.fetch_add(1, Ordering::Relaxed);
            if let Some(c) = &self.clock {
                c.advance(self.rules.stall_for);
            }
        }
    }

    fn decide(&self) -> Fault {
        if self.chance(self.rules.throttle) {
            self.faults.throttle.fetch_add(1, Ordering::Relaxed);
            return Fault::Throttle;
        }
        if self.chance(self.rules.fail_before) {
            self.faults.fail_before.fetch_add(1, Ordering::Relaxed);
            return if self.chance(0.5) {
                Fault::Before(Class::Unavailable, "connection refused")
            } else {
                Fault::Before(Class::Ambiguous, "request timed out")
            };
        }
        if self.chance(self.rules.fail_after) {
            self.faults.fail_after.fetch_add(1, Ordering::Relaxed);
            return Fault::After(if self.chance(0.5) {
                "operation timed out (response lost)"
            } else {
                "connection reset by peer (response lost)"
            });
        }
        Fault::None
    }

    /// Whether this conditional write overlaps another client's and gets a 409, and if so
    /// whether it takes effect.
    fn overlap(&self, path: &Path) -> Option<bool> {
        let (s, client) = self.sched.as_ref()?;
        if s.overlapping_conditional(*client, path.as_ref()) && self.chance(self.rules.overlap_409)
        {
            self.faults.overlap_409.fetch_add(1, Ordering::Relaxed);
            Some(self.chance(0.5))
        } else {
            None
        }
    }

    fn err(class: Class, detail: &str) -> object_store::Error {
        StoreFault::new(class, format!("injected: {detail}")).into_store_error()
    }

    /// Runs a mutating request under the fault rules.
    async fn mutate<T, F>(&self, path: &Path, conditional: bool, f: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        if let Some(applied) = conditional.then(|| self.overlap(path)).flatten() {
            if applied {
                f.await?;
            }
            return Err(Self::err(
                Class::Ambiguous,
                "409 ConditionalRequestConflict (overlapping conditional writes)",
            ));
        }
        let fault = match self.decide() {
            Fault::None
                if conditional
                    && path.as_ref().contains("/_delta_log/")
                    && self.chance(self.rules.ambiguous_commits) =>
            {
                self.faults.fail_after.fetch_add(1, Ordering::Relaxed);
                Fault::After("operation timed out (response lost)")
            }
            Fault::None
                if conditional
                    && path.as_ref().contains("/_delta_log/")
                    && self.chance(self.rules.generic_after_commits) =>
            {
                self.faults.fail_after.fetch_add(1, Ordering::Relaxed);
                Fault::GenericAfter
            }
            other => other,
        };
        match fault {
            Fault::None => f.await,
            Fault::Throttle => Err(Self::err(Class::Throttled, "503 SlowDown")),
            Fault::Before(class, d) => Err(Self::err(class, d)),
            Fault::After(d) => {
                f.await?;
                Err(Self::err(Class::Ambiguous, d))
            }
            Fault::GenericAfter => {
                f.await?;
                Err(object_store::Error::Generic {
                    store: "chaos",
                    source: "injected: the server returned an unrecognised response".into(),
                })
            }
        }
    }

    /// Runs a read under the fault rules (an "after" fault on a read is just an error).
    async fn read<T, F>(&self, f: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        match self.decide() {
            Fault::None => f.await,
            Fault::Throttle => Err(Self::err(Class::Throttled, "503 SlowDown")),
            Fault::Before(class, d) => Err(Self::err(class, d)),
            Fault::After(d) => {
                let _ = f.await;
                Err(Self::err(Class::Ambiguous, d))
            }
            Fault::GenericAfter => {
                let _ = f.await;
                Err(Self::err(Class::Ambiguous, "generic"))
            }
        }
    }
}

#[async_trait]
impl ObjectStore for ChaosStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> Result<PutResult> {
        let conditional = !matches!(opts.mode, PutMode::Overwrite);
        self.enter(Op::of_put(&opts.mode), location, conditional)
            .await;
        self.requests.add_written(payload.content_length() as u64);
        if conditional {
            match self.rules.conditional {
                Conditional::Honour => {}
                Conditional::Reject => {
                    return Err(object_store::Error::NotImplemented {
                        operation: "conditional put".into(),
                        implementer: self.to_string(),
                    });
                }
                Conditional::Ignore => opts.mode = PutMode::Overwrite,
            }
        }
        self.mutate(
            location,
            conditional,
            self.inner.put_opts(location, payload, opts),
        )
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.enter(Op::Multipart, location, false).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let op = Op::of_get(&options);
        self.enter(op, location, false).await;
        let head = options.head;
        let mut r = self.read(self.inner.get_opts(location, options)).await?;
        if head {
            return Ok(r);
        }
        let body: Bytes = match r.payload {
            GetResultPayload::Stream(s) => s.try_collect::<Vec<Bytes>>().await?.concat().into(),
            #[allow(unreachable_patterns)]
            other => {
                r.payload = other;
                return Ok(r);
            }
        };
        self.requests.add_read(body.len() as u64);
        let body = if !body.is_empty() && self.chance(self.rules.truncate) {
            self.faults.truncate.fetch_add(1, Ordering::Relaxed);
            let keep = self.rng.lock().expect("rng lock").below(body.len() as u64);
            body.slice(..keep as usize)
        } else {
            body
        };
        r.payload =
            GetResultPayload::Stream(futures::stream::once(async move { Ok(body) }).boxed());
        Ok(r)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // Deletes are gated and faulted one path at a time, in order.
        let me = self.shallow();
        locations
            .then(move |p| {
                let me = me.clone();
                async move {
                    let p = p?;
                    me.enter(Op::Delete, &p, false).await;
                    let inner = me.inner.clone();
                    let one = p.clone();
                    me.mutate(&p, false, async move {
                        let s = futures::stream::once(async move { Ok(one) }).boxed();
                        inner
                            .delete_stream(s)
                            .next()
                            .await
                            .unwrap_or_else(|| Err(Self::err(Class::Permanent, "no result")))
                    })
                    .await
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let me = self.shallow();
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            let at = prefix.clone().unwrap_or_default();
            me.enter(Op::List, &at, false).await;
            let items: Result<Vec<ObjectMeta>> = me
                .read(me.inner.list(prefix.as_ref()).try_collect::<Vec<_>>())
                .await;
            let mut out: Vec<Result<ObjectMeta>> = match items {
                Ok(v) => v.into_iter().map(Ok).collect(),
                Err(e) => vec![Err(e)],
            };
            if out.iter().all(|r| r.is_ok()) && me.chance(me.rules.cut_listing) {
                me.faults.cut_listing.fetch_add(1, Ordering::Relaxed);
                let keep = me.rng.lock().expect("rng lock").below(out.len() as u64 + 1);
                out.truncate(keep as usize);
                out.push(Err(Self::err(
                    Class::Ambiguous,
                    "listing failed part-way (next page timed out)",
                )));
            }
            futures::stream::iter(out)
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let at = prefix.cloned().unwrap_or_default();
        self.enter(Op::ListDelimiter, &at, false).await;
        self.read(self.inner.list_with_delimiter(prefix)).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let conditional = matches!(options.mode, CopyMode::Create);
        let op = if conditional {
            Op::CopyIfNotExists
        } else {
            Op::Copy
        };
        self.enter(op, to, conditional).await;
        self.mutate(to, conditional, self.inner.copy_opts(from, to, options))
            .await
    }
}

impl ChaosStore {
    /// A handle sharing everything, for streams that outlive `&self`.
    fn shallow(&self) -> Arc<ChaosStore> {
        Arc::new(ChaosStore {
            inner: self.inner.clone(),
            rules: self.rules.clone(),
            // Streams draw from a generator forked off this one, so the sequence stays
            // deterministic.
            rng: Mutex::new(self.rng.lock().expect("rng lock").fork(0x5EED)),
            sched: self.sched.clone(),
            clock: self.clock.clone(),
            requests: self.requests.clone(),
            faults: self.faults.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SimStore;
    use moraine::testing::classify;
    use object_store::ObjectStoreExt;

    fn store(rules: FaultRules) -> (ChaosStore, SimStore) {
        let sim = SimStore::new(SimClock::new());
        (ChaosStore::new(Arc::new(sim.clone()), rules, 1), sim)
    }

    #[tokio::test]
    async fn fail_after_applies_and_reports_ambiguous() {
        let (c, sim) = store(FaultRules {
            fail_after: 1.0,
            ..Default::default()
        });
        let e = c.put(&Path::from("x"), "1".into()).await.unwrap_err();
        assert_eq!(classify(&e), Class::Ambiguous);
        assert_eq!(sim.peek("x").as_deref(), Some(&b"1"[..]));
        assert_eq!(c.faults().ambiguous(), 1);
    }

    #[tokio::test]
    async fn fail_before_does_not_apply() {
        let (c, sim) = store(FaultRules {
            fail_before: 1.0,
            ..Default::default()
        });
        assert!(c.put(&Path::from("x"), "1".into()).await.is_err());
        assert!(sim.peek("x").is_none());
    }

    #[tokio::test]
    async fn ignored_conditions_overwrite() {
        let (c, sim) = store(FaultRules {
            conditional: Conditional::Ignore,
            ..Default::default()
        });
        let create = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        c.put_opts(&Path::from("x"), "1".into(), create.clone())
            .await
            .unwrap();
        c.put_opts(&Path::from("x"), "2".into(), create)
            .await
            .unwrap();
        assert_eq!(sim.peek("x").as_deref(), Some(&b"2"[..]));
    }

    #[tokio::test]
    async fn truncation_and_cut_listings() {
        let (c, _) = store(FaultRules {
            truncate: 1.0,
            cut_listing: 1.0,
            ..Default::default()
        });
        c.put(&Path::from("d/x"), "hello".into()).await.unwrap();
        let r = c.get(&Path::from("d/x")).await.unwrap();
        assert!(r.bytes().await.unwrap().len() < 5);
        let l: Vec<_> = c.list(Some(&Path::from("d"))).collect().await;
        assert!(l.last().unwrap().is_err());
    }
}
