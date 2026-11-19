//! The store stack moraine opens a database with.

use std::sync::Arc;

use object_store::ObjectStore;

use super::{MetricsStore, RetryPolicy, RetryStore, StoreMetrics, TagStore};
use crate::clock::Clock;

/// The store stack moraine uses: retry over metrics over a backend, plus the clock both use.
#[derive(Clone, Debug)]
pub struct Store {
    object: Arc<dyn ObjectStore>,
    metrics: Arc<StoreMetrics>,
    clock: Arc<dyn Clock>,
}

impl Store {
    /// Wraps `backend` with request metrics and the retry policy.
    pub fn new(backend: Arc<dyn ObjectStore>, clock: Arc<dyn Clock>, policy: RetryPolicy) -> Self {
        Self::with_tags(backend, clock, policy, Vec::new())
    }

    /// As [`Store::new`], adding `tags` to every object written (no wrapper when empty).
    pub fn with_tags(
        backend: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
        policy: RetryPolicy,
        tags: Vec<(String, String)>,
    ) -> Self {
        let backend: Arc<dyn ObjectStore> = if tags.is_empty() {
            backend
        } else {
            Arc::new(TagStore::new(backend, tags))
        };
        let metrics = Arc::new(StoreMetrics::default());
        let counted: Arc<dyn ObjectStore> = Arc::new(MetricsStore::new(backend, metrics.clone()));
        let object: Arc<dyn ObjectStore> =
            Arc::new(RetryStore::new(counted, clock.clone(), policy));
        Store {
            object,
            metrics,
            clock,
        }
    }

    /// The top of the stack, to pass to anything that takes an `ObjectStore`.
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object
    }

    /// Request and byte counters of everything sent through this store.
    pub fn metrics(&self) -> &Arc<StoreMetrics> {
        &self.metrics
    }

    /// The clock retries and server-time probes use.
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }
}
