//! The object store's clock. Expiry decisions compare object ages with server
//! time, so a client clock that is wrong by hours changes nothing (case G1).

use std::time::Duration;

use chrono::{DateTime, Utc};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::clock::{Clock, random_name};
use crate::error::Result;

/// Server time read once, advanced afterwards with the client's monotonic clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerClock {
    /// Last-modified time of the probe object, as the server recorded it.
    pub server: DateTime<Utc>,
    /// The client's monotonic time when the probe completed.
    pub at: Duration,
}

impl ServerClock {
    /// Server time now, as `server` plus the monotonic time elapsed since the probe.
    ///
    /// The probe's own latency makes this early by at most one round trip, and S3 reports
    /// last-modified times in whole seconds, so it can also be early by up to one second. Both
    /// errors make objects look younger, which is the safe direction for collectors.
    pub fn now(&self, clock: &dyn Clock) -> DateTime<Utc> {
        let elapsed = clock.now().saturating_sub(self.at);
        self.server + chrono::Duration::from_std(elapsed).unwrap_or(chrono::Duration::zero())
    }
}

/// Writes a tiny object under `<prefix>/clock/`, reads its last-modified time and deletes it.
pub async fn server_now(
    store: &dyn ObjectStore,
    prefix: &Path,
    clock: &dyn Clock,
) -> Result<ServerClock> {
    let path = prefix.clone().join("clock").join(random_name(clock));
    store.put(&path, PutPayload::from_static(b"t")).await?;
    let meta = store.head(&path).await;
    let at = clock.now();
    let _ = store.delete(&path).await;
    Ok(ServerClock {
        server: meta?.last_modified,
        at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::SystemClock;
    use futures::StreamExt;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn reads_server_time_and_cleans_up() {
        let s = InMemory::new();
        let clock = SystemClock::new();
        let before = Utc::now();
        let t = server_now(&s, &Path::from("meta"), &clock).await.unwrap();
        assert!(t.server >= before - chrono::Duration::seconds(1));
        assert!(t.now(&clock) >= t.server);
        assert!(s.list(None).next().await.is_none());
    }
}
