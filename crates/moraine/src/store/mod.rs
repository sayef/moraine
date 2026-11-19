//! The store layer: what a database's object store is, and what moraine counts on it.
//!
//! moraine opens every database on an [`ObjectStore`] and stacks its own wrappers on it:
//! retries with backoff that never repeat a conditional write whose outcome is unknown (case
//! S1), request metrics ([`StoreMetrics`], D18), and the database's object tags (D29). A local
//! directory is wrapped in [`LocalCasStore`], which adds compare-and-swap to the filesystem
//! (D15).
//!
//! What callers configure and read: [`RetryPolicy`] (through
//! [`DatabaseBuilder::request_retry`](crate::DatabaseBuilder::request_retry) and
//! [`commit_retry`](crate::DatabaseBuilder::commit_retry)) and the counters of
//! [`Database::store_metrics`](crate::Database::store_metrics).

#[cfg(feature = "aws-sdk")]
pub(crate) mod aws;
pub(crate) mod class;
mod local_cas;
pub(crate) mod metrics;
pub(crate) mod probe;
pub(crate) mod retry;
pub(crate) mod stack;
pub(crate) mod tags;
pub(crate) mod time;

pub use local_cas::LocalCasStore;
pub use metrics::{MetricsSnapshot, Op, OpCount, StoreMetrics};
pub use object_store::{ObjectStore, path::Path};
pub use retry::RetryPolicy;

pub(crate) use class::{Class, StoreFault, classify};
pub(crate) use metrics::MetricsStore;
pub(crate) use probe::probe_conditional_writes;
pub(crate) use retry::RetryStore;
pub(crate) use stack::Store;
pub(crate) use tags::TagStore;
pub(crate) use time::{ServerClock, server_now};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_typed_and_faults() {
        let nf = object_store::Error::NotFound {
            path: "a".into(),
            source: "x".into(),
        };
        assert_eq!(classify(&nf), Class::NotFound);
        let f = StoreFault::new(Class::Ambiguous, "lost response").into_store_error();
        assert_eq!(classify(&f), Class::Ambiguous);
        let nested = object_store::Error::Generic {
            store: "S3",
            source: Box::new(std::io::Error::other(StoreFault::new(
                Class::Throttled,
                "x",
            ))),
        };
        assert_eq!(classify(&nested), Class::Throttled);
    }

    #[test]
    fn classify_http_messages() {
        let g = |m: &str| object_store::Error::Generic {
            store: "S3",
            source: m.to_string().into(),
        };
        assert_eq!(
            classify(&g("Server returned 503 SlowDown")),
            Class::Throttled
        );
        assert_eq!(
            classify(&g("error sending request: operation timed out")),
            Class::Ambiguous
        );
        assert_eq!(
            classify(&g("409 ConditionalRequestConflict")),
            Class::Ambiguous
        );
        assert_eq!(
            classify(&g("tcp connect error: Connection refused")),
            Class::Unavailable
        );
        assert_eq!(classify(&g("403 AccessDenied")), Class::Permanent);
    }
}
