//! Retries with exponential backoff and full jitter (case S3).
//!
//! Which requests are repeated depends on whether repeating them is safe:
//!
//! - Reads, listings, overwrites, deletes and plain copies are idempotent and are retried on any
//!   transient class ([`Class::retry_idempotent`]).
//! - Conditional writes (`PutMode::Create`, `PutMode::Update`, copy-if-not-exists) are retried
//!   only when the store certainly did not process them ([`Class::retry_conditional`]). An
//!   ambiguous outcome is returned to the caller unchanged: repeating a create whose response was
//!   lost would report "already exists" for the caller's own write (case S1).
//! - Multipart uploads are passed through; their parts carry their own retries in the backends.
//!
//! A read whose body is shorter than the response announced (case S13) is retried. Listings are
//! collected completely before anything is yielded; a listing that keeps failing is an error,
//! never a shorter answer (case S4).

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyMode, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result,
};

use super::{Class, StoreFault, classify};
use crate::clock::Clock;

/// Exponential backoff with full jitter, an attempt limit and an overall deadline. Start from
/// [`RetryPolicy::REQUEST`], [`RetryPolicy::COMMIT`] or [`RetryPolicy::NONE`] and change it
/// with the setters.
///
/// ```
/// use std::time::Duration;
/// use moraine::store::RetryPolicy;
///
/// let patient = RetryPolicy::COMMIT.max_attempts(40).deadline(Duration::from_secs(120));
/// assert_eq!(patient.max_attempts, 40);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// Delay bound of the first retry.
    pub base: Duration,
    /// Growth of the delay bound per attempt.
    pub factor: u32,
    /// Upper limit of the delay bound.
    pub cap: Duration,
    /// Attempts in total, the first one included.
    pub max_attempts: u32,
    /// No retry starts after this much time since the first attempt.
    pub deadline: Duration,
}

impl RetryPolicy {
    /// The commit retry defaults: 50 ms doubling to 5 s, 20 attempts, 60 s.
    pub const COMMIT: RetryPolicy = RetryPolicy {
        base: Duration::from_millis(50),
        factor: 2,
        cap: Duration::from_secs(5),
        max_attempts: 20,
        deadline: Duration::from_secs(60),
    };

    /// Defaults for single requests: 50 ms doubling to 5 s, 10 attempts, 60 s.
    pub const REQUEST: RetryPolicy = RetryPolicy {
        base: Duration::from_millis(50),
        factor: 2,
        cap: Duration::from_secs(5),
        max_attempts: 10,
        deadline: Duration::from_secs(60),
    };

    /// No retries.
    pub const NONE: RetryPolicy = RetryPolicy {
        base: Duration::ZERO,
        factor: 1,
        cap: Duration::ZERO,
        max_attempts: 1,
        deadline: Duration::ZERO,
    };

    /// The delay bound of the first retry.
    pub const fn base(mut self, d: Duration) -> Self {
        self.base = d;
        self
    }

    /// Growth of the delay bound per attempt.
    pub const fn factor(mut self, factor: u32) -> Self {
        self.factor = factor;
        self
    }

    /// Upper limit of the delay bound.
    pub const fn cap(mut self, d: Duration) -> Self {
        self.cap = d;
        self
    }

    /// Attempts in total, the first one included.
    pub const fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts;
        self
    }

    /// No retry starts after this much time since the first attempt.
    pub const fn deadline(mut self, d: Duration) -> Self {
        self.deadline = d;
        self
    }

    /// The delay before retry number `retry` (1 for the first retry): uniform in
    /// `[0, min(cap, base * factor^(retry - 1))]`, drawn from `random`.
    pub fn delay(&self, retry: u32, random: u64) -> Duration {
        let exp = retry.saturating_sub(1).min(30);
        let bound = self
            .base
            .saturating_mul(self.factor.saturating_pow(exp))
            .min(self.cap);
        let nanos = bound.as_nanos() as u64;
        if nanos == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos(random % (nanos + 1))
    }

    /// Whether another attempt may start after `attempts` attempts and `elapsed` time.
    pub fn may_retry(&self, attempts: u32, elapsed: Duration) -> bool {
        attempts < self.max_attempts && elapsed < self.deadline
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy::REQUEST
    }
}

/// Retries transient failures of the wrapped store; see the module documentation.
#[derive(Debug)]
pub struct RetryStore {
    inner: Arc<dyn ObjectStore>,
    clock: Arc<dyn Clock>,
    policy: RetryPolicy,
}

impl RetryStore {
    /// Wraps `inner`, retrying per `policy` with backoff jitter from `clock`.
    pub fn new(inner: Arc<dyn ObjectStore>, clock: Arc<dyn Clock>, policy: RetryPolicy) -> Self {
        RetryStore {
            inner,
            clock,
            policy,
        }
    }

    /// Runs `f` until it succeeds, fails with a class `retryable` rejects, or the policy is
    /// exhausted. `f` gets the attempt number (0 first).
    async fn retry<T, F, Fut>(&self, retryable: fn(Class) -> bool, mut f: F) -> Result<T>
    where
        F: FnMut(u32) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let start = self.clock.now();
        let mut attempt = 0u32;
        loop {
            let r = f(attempt).await;
            attempt += 1;
            match r {
                Ok(v) => return Ok(v),
                Err(e) => {
                    let elapsed = self.clock.now().saturating_sub(start);
                    if !retryable(classify(&e)) || !self.policy.may_retry(attempt, elapsed) {
                        return Err(e);
                    }
                    let d = self.policy.delay(attempt, self.clock.random_u64());
                    self.clock.sleep(d).await;
                }
            }
        }
    }
}

impl fmt::Display for RetryStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Retry({})", self.inner)
    }
}

/// Reads a whole body and checks its length against the announced range.
async fn read_checked(r: GetResult) -> Result<GetResult> {
    let GetResult {
        payload,
        meta,
        range,
        attributes,
    } = r;
    let stream = match payload {
        GetResultPayload::Stream(s) => s,
        #[allow(unreachable_patterns)]
        other => {
            return Ok(GetResult {
                payload: other,
                meta,
                range,
                attributes,
            });
        }
    };
    let want = range.end - range.start;
    let mut buf = BytesMut::with_capacity(want.min(64 << 20) as usize);
    let mut s = stream;
    while let Some(chunk) = s.try_next().await? {
        buf.extend_from_slice(&chunk);
    }
    if buf.len() as u64 != want {
        return Err(StoreFault::new(
            Class::Corrupt,
            format!(
                "{}: body has {} bytes, expected {want}",
                meta.location,
                buf.len()
            ),
        )
        .into_store_error());
    }
    let body: Bytes = buf.freeze();
    Ok(GetResult {
        payload: GetResultPayload::Stream(futures::stream::once(async move { Ok(body) }).boxed()),
        meta,
        range,
        attributes,
    })
}

#[async_trait]
impl ObjectStore for RetryStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let retryable: fn(Class) -> bool = match opts.mode {
            PutMode::Overwrite => Class::retry_idempotent,
            PutMode::Create | PutMode::Update(_) => Class::retry_conditional,
        };
        self.retry(retryable, |_| {
            self.inner.put_opts(location, payload.clone(), opts.clone())
        })
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.retry(Class::retry_idempotent, |_| {
            let options = options.clone();
            async move {
                let head = options.head;
                let r = self.inner.get_opts(location, options).await?;
                if head { Ok(r) } else { read_checked(r).await }
            }
        })
        .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.retry(Class::retry_idempotent, |_| async move {
            let out = self.inner.get_ranges(location, ranges).await?;
            for (b, r) in out.iter().zip(ranges) {
                if b.len() as u64 != r.end - r.start {
                    return Err(StoreFault::new(
                        Class::Corrupt,
                        format!("{location}: range {r:?} returned {} bytes", b.len()),
                    )
                    .into_store_error());
                }
            }
            Ok(out)
        })
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // Each path is deleted on its own so it can be retried on its own. A path that is gone
        // after an ambiguous attempt counts as deleted.
        let inner = self.inner.clone();
        let clock = self.clock.clone();
        let policy = self.policy;
        locations
            .then(move |p| {
                let inner = inner.clone();
                let clock = clock.clone();
                async move {
                    let p = p?;
                    let me = RetryStore::new(inner, clock, policy);
                    let mut retried = false;
                    me.retry(Class::retry_idempotent, |attempt| {
                        retried |= attempt > 0;
                        let inner = me.inner.clone();
                        let p = p.clone();
                        let after_retry = retried;
                        async move {
                            let one = futures::stream::once(async move { Ok(p) }).boxed();
                            match inner.delete_stream(one).next().await {
                                Some(Ok(done)) => Ok(done),
                                Some(Err(object_store::Error::NotFound { path, .. }))
                                    if after_retry =>
                                {
                                    Ok(Path::from(path))
                                }
                                Some(Err(e)) => Err(e),
                                None => Err(StoreFault::new(
                                    Class::Permanent,
                                    "delete stream ended without a result",
                                )
                                .into_store_error()),
                            }
                        }
                    })
                    .await
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let inner = self.inner.clone();
        let clock = self.clock.clone();
        let policy = self.policy;
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            let me = RetryStore::new(inner, clock, policy);
            me.retry(Class::retry_idempotent, |_| {
                me.inner.list(prefix.as_ref()).try_collect::<Vec<_>>()
            })
            .await
        })
        .map(|r| match r {
            Ok(v) => futures::stream::iter(v.into_iter().map(Ok)).boxed(),
            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
        })
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let inner = self.inner.clone();
        let clock = self.clock.clone();
        let policy = self.policy;
        let prefix = prefix.cloned();
        let offset = offset.clone();
        futures::stream::once(async move {
            let me = RetryStore::new(inner, clock, policy);
            me.retry(Class::retry_idempotent, |_| {
                me.inner
                    .list_with_offset(prefix.as_ref(), &offset)
                    .try_collect::<Vec<_>>()
            })
            .await
        })
        .map(|r| match r {
            Ok(v) => futures::stream::iter(v.into_iter().map(Ok)).boxed(),
            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.retry(Class::retry_idempotent, |_| {
            self.inner.list_with_delimiter(prefix)
        })
        .await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let retryable: fn(Class) -> bool = match options.mode {
            CopyMode::Overwrite => Class::retry_idempotent,
            CopyMode::Create => Class::retry_conditional,
        };
        self.retry(retryable, |_| {
            self.inner.copy_opts(from, to, options.clone())
        })
        .await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        // A rename is a copy and a delete in most backends; neither half is repeated blindly.
        self.retry(Class::retry_conditional, |_| {
            self.inner.rename_opts(from, to, options.clone())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_is_bounded_and_grows() {
        let p = RetryPolicy::COMMIT;
        assert_eq!(p.delay(1, 50_000_000), Duration::from_millis(50));
        for r in [1u64, 7, 1 << 40, u64::MAX] {
            assert!(p.delay(1, r) <= Duration::from_millis(50));
            assert!(p.delay(2, r) <= Duration::from_millis(100));
            assert!(p.delay(30, r) <= Duration::from_secs(5));
        }
        assert_eq!(p.delay(30, 5_000_000_000), Duration::from_secs(5));
        assert_eq!(p.delay(3, 0), Duration::ZERO);
        assert!(p.may_retry(19, Duration::from_secs(1)));
        assert!(!p.may_retry(20, Duration::from_secs(1)));
        assert!(!p.may_retry(1, Duration::from_secs(60)));
    }
}
