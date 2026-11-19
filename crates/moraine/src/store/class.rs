//! How object-store errors are classified for retries and commits.

use std::fmt;

/// How an object-store error should be handled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Class {
    /// The object does not exist.
    NotFound,
    /// A `PutMode::Create` (or copy-if-not-exists) target exists.
    AlreadyExists,
    /// A `PutMode::Update` or `if_match` precondition failed.
    Precondition,
    /// An `if_none_match` read found the object unchanged.
    NotModified,
    /// 429 or 503 SlowDown: the request was refused before it took effect. Retry with backoff.
    Throttled,
    /// The request was certainly not processed (connection refused, DNS failure). Retry.
    Unavailable,
    /// The request may or may not have taken effect: a timeout, a reset connection, a 5xx after
    /// sending, or a 409 from overlapping conditional writes. Safe to retry only when the
    /// operation is idempotent.
    Ambiguous,
    /// A response body shorter than announced (case S13). Retry the read.
    Corrupt,
    /// Anything else: permissions, configuration, unsupported operations, bad paths.
    Permanent,
}

impl Class {
    /// Whether an idempotent request (a read, an overwrite, a delete) may be repeated.
    pub fn retry_idempotent(self) -> bool {
        matches!(
            self,
            Class::Throttled | Class::Unavailable | Class::Ambiguous | Class::Corrupt
        )
    }

    /// Whether a conditional write may be repeated as it is: only when it certainly did not
    /// take effect.
    pub fn retry_conditional(self) -> bool {
        matches!(self, Class::Throttled | Class::Unavailable)
    }
}

/// An error with a known [`Class`]. Fault-injecting test stores return it inside
/// `object_store::Error::Generic`, so classification needs no guessing; the retry wrapper uses it
/// for bodies that arrive truncated.
#[derive(Debug, Clone)]
pub struct StoreFault {
    /// How the error is handled.
    pub class: Class,
    /// What happened, for messages.
    pub detail: String,
}

impl StoreFault {
    /// A fault of `class`.
    pub fn new(class: Class, detail: impl Into<String>) -> Self {
        StoreFault {
            class,
            detail: detail.into(),
        }
    }

    /// Wraps the fault as the `Generic` object-store error stores return.
    pub fn into_store_error(self) -> object_store::Error {
        object_store::Error::Generic {
            store: "moraine",
            source: Box::new(self),
        }
    }
}

impl fmt::Display for StoreFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.class, self.detail)
    }
}

impl std::error::Error for StoreFault {}

/// Classifies an object-store error.
///
/// Typed variants map directly. `Generic` errors carrying a [`StoreFault`] anywhere in their
/// source chain take its class. Other `Generic` errors come from HTTP clients and are recognised
/// by their message; anything unrecognised is [`Class::Ambiguous`] if it mentions the transport
/// and [`Class::Permanent`] otherwise.
pub fn classify(e: &object_store::Error) -> Class {
    use object_store::Error as E;
    match e {
        E::NotFound { .. } => Class::NotFound,
        E::AlreadyExists { .. } => Class::AlreadyExists,
        E::Precondition { .. } => Class::Precondition,
        E::NotModified { .. } => Class::NotModified,
        E::Generic { source, .. } => {
            let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(source.as_ref());
            while let Some(err) = cur {
                if let Some(fault) = err.downcast_ref::<StoreFault>() {
                    return fault.class;
                }
                cur = err.source();
            }
            classify_message(&error_chain_text(source.as_ref()))
        }
        _ => Class::Permanent,
    }
}

fn error_chain_text(e: &(dyn std::error::Error + 'static)) -> String {
    let mut out = e.to_string();
    let mut cur = e.source();
    while let Some(err) = cur {
        out.push_str(": ");
        out.push_str(&err.to_string());
        cur = err.source();
    }
    out
}

fn classify_message(msg: &str) -> Class {
    let m = msg.to_ascii_lowercase();
    let has = |needles: &[&str]| needles.iter().any(|n| m.contains(n));
    if has(&[
        "429",
        "too many requests",
        "slowdown",
        "slow down",
        "503",
        "throttl",
    ]) {
        Class::Throttled
    } else if has(&[
        "connection refused",
        "dns error",
        "failed to lookup address",
    ]) {
        Class::Unavailable
    } else if has(&[
        "409",
        "conditionalrequestconflict",
        "timed out",
        "timeout",
        "connection reset",
        "connection closed",
        "broken pipe",
        "error sending request",
        "incomplete message",
        "500",
        "502",
        "504",
        "internalerror",
    ]) {
        Class::Ambiguous
    } else {
        Class::Permanent
    }
}
