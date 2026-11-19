//! Advisory leases as generation files (D29).
//!
//! A lease is the sequence `meta/leases/<job>/<generation>.json`. Every change (taking it,
//! renewing it, releasing it) creates the next generation with put-if-absent, so exactly one
//! contender wins each generation on any store, and no compare-and-swap is needed. A record
//! names its holder, its time to live and whether it was released; it expires a time to live
//! after its object's server modification time, so client clocks play no part. The generation
//! is a fencing token: a job's commits carry it as a Delta transaction (`appId` = the lease's
//! fence id, `version` = the generation), and a commit whose generation is older than one
//! already in the log is refused.
//!
//! A lease is a hint. Every job stays correct when two clients run it at once.

use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::log::TxnId;

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    holder: String,
    ttl_ms: u64,
    released: bool,
}

/// A lease this handle holds.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Lease {
    /// The job the lease is for, such as `maintain` or `ingest`.
    pub job: String,
    /// The generation this holder created: its fencing token.
    pub generation: u64,
    /// Server time after which another client may take it over.
    pub expires: DateTime<Utc>,
}

impl Lease {
    /// The transaction that fences this holder's commits.
    pub fn fence(&self) -> TxnId {
        TxnId::new(fence_id(&self.job), self.generation as i64)
    }
}

/// The Delta transaction application id that fences commits of `job`.
pub(crate) fn fence_id(job: &str) -> String {
    format!("moraine.lease.{job}")
}

/// The lease objects of one table.
pub(crate) struct Leases<'a> {
    pub store: &'a dyn ObjectStore,
    /// `<db>/<table>/meta/leases`
    pub prefix: Path,
    pub holder: &'a str,
}

/// Parses the record of generation `generation` of `job`'s lease.
fn parse_record(job: &str, generation: u64, body: &[u8]) -> Result<Record> {
    serde_json::from_slice(body).map_err(|e| Error::corrupt(format!("lease {job}/{generation}"), e))
}

/// Parses a lease generation object as acquiring the lease does: its name gives the
/// generation, its body the record. Returns the record's holder and time to live.
pub(crate) fn parse_generation(location: &str, body: &[u8]) -> Result<(u64, String, u64)> {
    let path = Path::from(location);
    let g = generation_of(&path)
        .ok_or_else(|| Error::corrupt(format!("lease {location}"), "not a generation file"))?;
    let r = parse_record("fuzz", g, body)?;
    Ok((g, r.holder, r.ttl_ms))
}

fn generation_of(location: &Path) -> Option<u64> {
    let name = location.filename()?;
    let stem = name.strip_suffix(".json")?;
    (stem.len() == 20).then(|| stem.parse().ok()).flatten()
}

impl Leases<'_> {
    fn location(&self, job: &str, generation: u64) -> Path {
        self.prefix
            .clone()
            .join(job)
            .join(format!("{generation:020}.json"))
    }

    /// Takes the lease of `job` when it is free, released or expired at server time `now`.
    /// `None` while another holder has it.
    pub async fn acquire(
        &self,
        job: &str,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<Option<Lease>> {
        let dir = self.prefix.clone().join(job);
        let listed: Vec<_> = self.store.list(Some(&dir)).try_collect().await?;
        let mut gens: Vec<(u64, DateTime<Utc>)> = listed
            .iter()
            .filter_map(|m| generation_of(&m.location).map(|g| (g, m.last_modified)))
            .collect();
        gens.sort();
        let current = gens.last().map(|(g, _)| *g).unwrap_or(0);
        if let Some(&(g, at)) = gens.last() {
            match self.store.get(&self.location(job, g)).await {
                Ok(r) => {
                    let body = r.bytes().await?;
                    let rec = parse_record(job, g, &body)?;
                    let ttl = chrono::Duration::milliseconds(rec.ttl_ms as i64);
                    if !rec.released && at + ttl > now && rec.holder != self.holder {
                        return Ok(None);
                    }
                }
                // A newer generation replaced it meanwhile: someone else is active.
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(e.into()),
            }
        }
        let next = self.advance(job, current, ttl, false, now).await?;
        // Generations older than the one just replaced are no longer needed.
        for (g, _) in gens.iter().rev().skip(1) {
            let _ = self.store.delete(&self.location(job, *g)).await;
        }
        Ok(next)
    }

    /// Extends a held lease. `false` when another holder took it over meanwhile.
    pub async fn renew(
        &self,
        lease: &mut Lease,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        match self
            .advance(&lease.job, lease.generation, ttl, false, now)
            .await?
        {
            Some(l) => {
                *lease = l;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Releases a held lease, so another client may take it at once.
    pub async fn release(&self, lease: &Lease, now: DateTime<Utc>) -> Result<()> {
        self.advance(&lease.job, lease.generation, Duration::ZERO, true, now)
            .await
            .map(|_| ())
    }

    /// Creates generation `from + 1`; `None` when another client created it first.
    async fn advance(
        &self,
        job: &str,
        from: u64,
        ttl: Duration,
        released: bool,
        now: DateTime<Utc>,
    ) -> Result<Option<Lease>> {
        let rec = Record {
            holder: self.holder.to_string(),
            ttl_ms: ttl.as_millis() as u64,
            released,
        };
        let body = serde_json::to_vec(&rec).map_err(|e| Error::Internal(e.to_string()))?;
        let next = from + 1;
        let at = self.location(job, next);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        let created = match self.store.put_opts(&at, PutPayload::from(body), opts).await {
            Ok(_) => true,
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => {
                // Ours when an earlier attempt of this very put took effect.
                self.holds(&at).await?
            }
            Err(e) => {
                if crate::store::classify(&e).retry_idempotent() {
                    self.holds(&at).await?
                } else {
                    return Err(e.into());
                }
            }
        };
        if !created {
            return Ok(None);
        }
        if from > 0 {
            let _ = self.store.delete(&self.location(job, from)).await;
        }
        Ok(Some(Lease {
            job: job.to_string(),
            generation: next,
            expires: now + chrono::Duration::from_std(ttl).unwrap_or_default(),
        }))
    }

    /// Whether the generation at `at` exists and names this holder.
    async fn holds(&self, at: &Path) -> Result<bool> {
        match self.store.get(at).await {
            Ok(r) => {
                let body = r.bytes().await?;
                Ok(parse_record("", 0, &body).is_ok_and(|r| r.holder == self.holder))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}
