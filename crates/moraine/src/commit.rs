//! The commit retry loop, independent of what is being
//! committed.
//!
//! The test kit's toy client (a counter object updated by compare-and-swap) and the Delta
//! commit path both run through [`commit_with_retries`], so the simulation that
//! checks the toy also checks the loop the table writes will use.

use std::time::Duration;

use futures::future::BoxFuture;

use crate::clock::Clock;
use crate::error::{Error, Result};
use crate::store::{Class, RetryPolicy, classify};

/// Why an attempt lost to another writer. Every kind means: reload, re-execute, try again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictKind {
    /// The version (or object version) the attempt wanted was taken.
    VersionTaken,
    /// A concurrent commit changed data the attempt read or rewrote.
    ConcurrentModification,
    /// A concurrent commit changed the table's metadata (schema or properties). delta-rs fails
    /// every write prepared before such a commit; D9 makes it a conflict like any other.
    MetadataChanged,
}

impl ConflictKind {
    /// Recognises a conflict in a commit-engine error message, `None` when it is not one.
    ///
    /// The messages are delta-rs's (`TransactionError` and `CommitConflictError` in the pinned
    /// version); the commit layer passes their `Display` text here so no delta-rs type crosses
    /// into the rest of moraine.
    pub fn from_message(msg: &str) -> Option<ConflictKind> {
        let m = msg.to_ascii_lowercase();
        if m.contains("metadata changed since last commit") {
            Some(ConflictKind::MetadataChanged)
        } else if m.contains("existing table version")
            || (m.contains("version") && m.contains("already exists"))
        {
            Some(ConflictKind::VersionTaken)
        } else if m.contains("concurrent")
            || m.contains("commit conflict")
            || m.contains("protocol changed")
        {
            Some(ConflictKind::ConcurrentModification)
        } else {
            None
        }
    }
}

/// How one attempt failed.
#[derive(Debug)]
pub enum AttemptError {
    /// Lost to another writer; re-execute against fresh state.
    Conflict(ConflictKind),
    /// The commit may or may not have taken effect. Resolve before anything else.
    Ambiguous(String),
    /// Refused before taking effect (429, 503). Retry the same attempt; not a conflict.
    Throttled,
    /// Anything else. Not retried.
    Fatal(Error),
}

impl AttemptError {
    /// Classifies a store error from the attempt's commit request (the conditional write).
    ///
    /// Only an error that says the write did not happen may end the attempt as a failure:
    /// a conflict, a refusal before the effect (throttling, an unreachable store), or a request
    /// the store rejected outright (no permission, an invalid path, an unsupported mode).
    /// Everything else, including a `Generic` error whose message names no known class, may
    /// have taken effect and is ambiguous: the loop looks the transaction id up before it
    /// reports anything, and returns [`Error::OutcomeUnknown`] if it cannot tell.
    pub fn from_store(e: object_store::Error) -> AttemptError {
        use object_store::Error as E;
        match classify(&e) {
            Class::AlreadyExists | Class::Precondition => {
                AttemptError::Conflict(ConflictKind::VersionTaken)
            }
            Class::Throttled | Class::Unavailable => AttemptError::Throttled,
            Class::Ambiguous | Class::Corrupt => AttemptError::Ambiguous(e.to_string()),
            Class::NotFound | Class::NotModified | Class::Permanent => match e {
                E::PermissionDenied { .. }
                | E::Unauthenticated { .. }
                | E::InvalidPath { .. }
                | E::NotImplemented { .. }
                | E::NotSupported { .. }
                | E::UnknownConfigurationKey { .. }
                | E::NotFound { .. } => AttemptError::Fatal(Error::from(e)),
                other => AttemptError::Ambiguous(other.to_string()),
            },
        }
    }
}

impl AttemptError {
    /// Whether a failed conditional write may still have taken effect: the cases
    /// [`AttemptError::from_store`] calls ambiguous.
    pub(crate) fn from_store_is_open(e: &object_store::Error) -> bool {
        use object_store::Error as E;
        match classify(e) {
            Class::AlreadyExists | Class::Precondition => false,
            Class::Throttled | Class::Unavailable => false,
            Class::Ambiguous | Class::Corrupt => true,
            Class::NotFound | Class::NotModified | Class::Permanent => !matches!(
                e,
                E::PermissionDenied { .. }
                    | E::Unauthenticated { .. }
                    | E::InvalidPath { .. }
                    | E::NotImplemented { .. }
                    | E::NotSupported { .. }
                    | E::UnknownConfigurationKey { .. }
                    | E::NotFound { .. }
            ),
        }
    }
}

/// Counts of what happened in one [`commit_with_retries`] call.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LoopStats {
    /// Attempts made, the first one included.
    pub attempts: u32,
    /// Attempts that lost the race to another writer.
    pub conflicts: u32,
    /// Attempts the store throttled.
    pub throttled: u32,
    /// Attempts whose outcome was unknown (the request may have taken effect).
    pub ambiguous: u32,
    /// Unknown outcomes that a lookup found had taken effect.
    pub resolved_found: u32,
}

/// Lookups of an ambiguous outcome that may fail before [`Error::OutcomeUnknown`].
const RESOLVE_ATTEMPTS: u32 = 5;

/// Runs `attempt` until it commits, following the table of commit outcomes (D23).
///
/// - `Ok`: done.
/// - `Conflict`: back off and call `attempt` again (it re-reads and re-executes).
/// - `Throttled`: back off and call `attempt` again; counted separately from conflicts.
/// - `Ambiguous`: call `resolve`, which looks for the transaction id from the base version on.
///   `Some` means the commit happened: return it. `None` means it did not: try again. `resolve`
///   is never skipped, so an append cannot land twice.
/// - `Fatal`: return the error, unless an earlier attempt was ambiguous, in which case `resolve`
///   decides first, because a returned error must mean "not visible".
///
/// When attempts or the deadline run out: [`Error::TooManyConflicts`] (or
/// [`Error::Throttled`] if no attempt conflicted), again only after any ambiguity is resolved.
/// If resolving keeps failing, [`Error::OutcomeUnknown`] names `txn`.
pub async fn commit_with_retries<T, A, R>(
    policy: &RetryPolicy,
    clock: &dyn Clock,
    txn: &str,
    mut attempt: A,
    mut resolve: R,
) -> (Result<T>, LoopStats)
where
    A: FnMut(u32) -> BoxFuture<'static, Result<T, AttemptError>>,
    R: FnMut() -> BoxFuture<'static, Result<Option<T>>>,
{
    let start = clock.now();
    let mut stats = LoopStats::default();
    let mut unresolved = false;
    loop {
        let r = attempt(stats.attempts).await;
        stats.attempts += 1;
        let outcome_is_open = match r {
            Ok(v) => return (Ok(v), stats),
            Err(AttemptError::Conflict(_)) => {
                stats.conflicts += 1;
                false
            }
            Err(AttemptError::Throttled) => {
                stats.throttled += 1;
                false
            }
            Err(AttemptError::Ambiguous(_)) => {
                stats.ambiguous += 1;
                true
            }
            Err(AttemptError::Fatal(e)) => {
                if unresolved {
                    return finish_unresolved(clock, policy, txn, &mut resolve, &mut stats, e)
                        .await;
                }
                return (Err(e), stats);
            }
        };
        if outcome_is_open {
            match resolve_with_retries(clock, policy, &mut resolve).await {
                Ok(Some(v)) => {
                    stats.resolved_found += 1;
                    return (Ok(v), stats);
                }
                Ok(None) => unresolved = false,
                Err(_) => unresolved = true,
            }
        }
        let elapsed = clock.now().saturating_sub(start);
        if !policy.may_retry(stats.attempts, elapsed) {
            let e = if stats.conflicts == 0 && stats.throttled > 0 {
                Error::Throttled {
                    attempts: stats.attempts,
                }
            } else {
                Error::TooManyConflicts {
                    attempts: stats.attempts,
                }
            };
            if unresolved {
                return finish_unresolved(clock, policy, txn, &mut resolve, &mut stats, e).await;
            }
            return (Err(e), stats);
        }
        clock
            .sleep(policy.delay(stats.attempts, clock.random_u64()))
            .await;
    }
}

async fn resolve_with_retries<T, R>(
    clock: &dyn Clock,
    policy: &RetryPolicy,
    resolve: &mut R,
) -> Result<Option<T>>
where
    R: FnMut() -> BoxFuture<'static, Result<Option<T>>>,
{
    let mut last = None;
    for i in 0..RESOLVE_ATTEMPTS {
        match resolve().await {
            Ok(v) => return Ok(v),
            Err(e) => last = Some(e),
        }
        let d: Duration = policy.delay(i + 1, clock.random_u64());
        clock.sleep(d).await;
    }
    Err(last.unwrap_or_else(|| Error::Internal("resolve did not run".into())))
}

async fn finish_unresolved<T, R>(
    clock: &dyn Clock,
    policy: &RetryPolicy,
    txn: &str,
    resolve: &mut R,
    stats: &mut LoopStats,
    otherwise: Error,
) -> (Result<T>, LoopStats)
where
    R: FnMut() -> BoxFuture<'static, Result<Option<T>>>,
{
    match resolve_with_retries(clock, policy, resolve).await {
        Ok(Some(v)) => {
            stats.resolved_found += 1;
            (Ok(v), *stats)
        }
        Ok(None) => (Err(otherwise), *stats),
        Err(_) => (
            Err(Error::OutcomeUnknown {
                txn: txn.to_string(),
            }),
            *stats,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_definitive_errors_end_a_commit_attempt() {
        let generic = object_store::Error::Generic {
            store: "s3",
            source: "the server returned an unrecognised response".into(),
        };
        assert!(matches!(
            AttemptError::from_store(generic),
            AttemptError::Ambiguous(_)
        ));
        let denied = object_store::Error::PermissionDenied {
            path: "p".into(),
            source: "no".into(),
        };
        assert!(matches!(
            AttemptError::from_store(denied),
            AttemptError::Fatal(_)
        ));
        let taken = object_store::Error::AlreadyExists {
            path: "p".into(),
            source: "exists".into(),
        };
        assert!(matches!(
            AttemptError::from_store(taken),
            AttemptError::Conflict(ConflictKind::VersionTaken)
        ));
    }

    #[test]
    fn metadata_changed_is_a_conflict() {
        // The text delta-rs 1.1.4 produces for a write prepared before a metadata commit.
        let msg = "Transaction failed: Metadata changed since last commit.";
        assert_eq!(
            ConflictKind::from_message(msg),
            Some(ConflictKind::MetadataChanged)
        );
        assert_eq!(
            ConflictKind::from_message("Tried committing existing table version: 7"),
            Some(ConflictKind::VersionTaken)
        );
        assert_eq!(
            ConflictKind::from_message(
                "Commit failed: a concurrent transactions deleted the same data your transaction deletes"
            ),
            Some(ConflictKind::ConcurrentModification)
        );
        assert_eq!(ConflictKind::from_message("permission denied"), None);
    }
}
