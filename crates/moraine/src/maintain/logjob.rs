//! Checkpoints and log cleanup (D13, D14, D18, D47).
//!
//! A checkpoint is written at the version in hand once `interval` log entries have
//! accumulated since the newest checkpoint, with put-if-absent, so two clients writing the same
//! version produce one file. Transaction ids that carry a `lastUpdated` (moraine's own group
//! ids) are left out once older than the grace period (D14); callers' ids, lease fences and
//! queued-write submitters carry none and stay.
//!
//! Cleanup removes log entries and checkpoints older than the grace period (server time) that
//! a newer checkpoint makes unnecessary, as delta-rs does: the safe checkpoint is the newest one
//! at or before the oldest entry still inside the retention window, and only objects older
//! than both it and the window go.

use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::error::Result;
use crate::log::{CheckpointState, LogObjectKind};
use crate::table::TableInner;

/// What the log job did in one pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LogReport {
    /// The version a checkpoint was written for, if any.
    pub checkpoint: Option<u64>,
    /// Log entries and checkpoints removed.
    pub objects_removed: usize,
}

impl TableInner {
    pub(crate) async fn checkpoint_and_clean(
        self: &Arc<Self>,
        interval: Option<u64>,
        now: DateTime<Utc>,
        clean: bool,
    ) -> Result<LogReport> {
        let mut report = LogReport::default();
        let snap = self.refresh().await?;
        let interval = interval.unwrap_or_else(|| {
            snap.meta
                .configuration
                .get("delta.checkpointInterval")
                .and_then(|v| v.parse().ok())
                .unwrap_or(20)
        });
        let mut objects = self.layer.list_log_objects().await?;
        let newest = objects
            .iter()
            .filter(|o| o.kind == LogObjectKind::Checkpoint)
            .map(|o| o.version)
            .max();
        let since = match newest {
            Some(c) => snap.version.saturating_sub(c),
            None => snap.version + 1,
        };
        if since >= interval.max(1) && newest.is_none_or(|c| c < snap.version) {
            let cutoff = (now - grace(&snap.def)).timestamp_millis();
            let txns = snap
                .txns
                .all()
                .into_iter()
                .filter(|(_, _, at)| at.is_none_or(|t| t >= cutoff))
                .collect();
            fail::fail_point!("moraine::checkpoint::before_write");
            if self
                .layer
                .write_checkpoint(&CheckpointState {
                    version: snap.version,
                    meta: &snap.meta,
                    files: &snap.files,
                    txns,
                })
                .await?
            {
                report.checkpoint = Some(snap.version);
                objects = self.layer.list_log_objects().await?;
                // A purge that committed after this pass read the log may have erased values
                // this checkpoint holds (D66). Its own checkpoint is newer: once that exists,
                // this one goes; before, the purge deletes it.
                let now = self.refresh().await?;
                if let Some(p) = crate::table::purged(&now.meta.configuration)?
                    && p > snap.version
                {
                    objects = self.layer.list_log_objects().await?;
                    if objects
                        .iter()
                        .any(|o| o.kind == LogObjectKind::Checkpoint && o.version >= p)
                    {
                        let own: Vec<_> = objects
                            .iter()
                            .filter(|o| {
                                o.kind == LogObjectKind::Checkpoint && o.version == snap.version
                            })
                            .map(|o| o.location.clone())
                            .collect();
                        self.layer.delete_log_objects(own).await?;
                        report.checkpoint = None;
                        objects = self.layer.list_log_objects().await?;
                    }
                }
            }
        }
        if !clean {
            return Ok(report);
        }
        let cutoff = now - grace(&snap.def);
        let in_window = objects
            .iter()
            .filter(|o| o.kind == LogObjectKind::Entry && o.last_modified >= cutoff)
            .map(|o| o.version)
            .min()
            .unwrap_or(snap.version);
        let keep = snap.version.min(in_window);
        let Some(safe) = objects
            .iter()
            .filter(|o| o.kind == LogObjectKind::Checkpoint && o.version <= keep)
            .map(|o| o.version)
            .max()
        else {
            return Ok(report);
        };
        // Checkpoints older than the newest purge may hold erased values: they go at once,
        // once a checkpoint at or after the purge exists (D66).
        let purged = crate::table::purged(&snap.meta.configuration)?.filter(|p| {
            objects
                .iter()
                .any(|o| o.kind == LogObjectKind::Checkpoint && o.version >= *p)
        });
        let gone: Vec<_> = objects
            .into_iter()
            .filter(|o| {
                (o.version < safe && o.last_modified < cutoff)
                    || (o.kind == LogObjectKind::Checkpoint
                        && purged.is_some_and(|p| o.version < p))
            })
            .map(|o| o.location)
            .collect();
        fail::fail_point!("moraine::cleanup::before_delete");
        report.objects_removed = self.layer.delete_log_objects(gone).await?;
        Ok(report)
    }
}

pub(crate) fn grace(def: &crate::options::TableDef) -> chrono::Duration {
    chrono::Duration::from_std(def.grace_period).unwrap_or(chrono::Duration::hours(1))
}
