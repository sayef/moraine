//! `verify` and `repair`.
//!
//! Verify checks, against one fresh snapshot, that every data file exists with its recorded
//! size and that every sidecar of a live file and a current index spec validates (header,
//! footer and section checksums). Repair removes from the table the files that are missing,
//! with one commit, and reports the rows lost. It exists for the one failure the design cannot
//! fully exclude: a writer frozen between its deadline check and its commit for longer than
//! three quarters of the grace period, whose files vacuum may have taken.

use std::collections::HashMap;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::ObjectStoreExt;

use crate::error::Result;
use crate::index::bundle::{BundleBytes, BundleView, Expect};
use crate::index::discover::{data_segment, parse};
use crate::index::spec::{parse_specs, spec_hash};
use crate::table::{ReplaceWork, TableInner};

/// What [`crate::Table::verify`] found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct VerifyReport {
    /// The version checked.
    pub version: u64,
    /// Data files checked.
    pub files_checked: usize,
    /// Data files the snapshot lists that the store does not hold.
    pub missing_files: Vec<String>,
    /// Data files whose stored size differs from the recorded one.
    pub wrong_size: Vec<String>,
    /// Sidecars checked.
    pub sidecars_checked: usize,
    /// Sidecars of live files and current specs that do not validate, with the reason.
    pub invalid_sidecars: Vec<(String, String)>,
    /// External blob references checked.
    pub blobs_checked: usize,
    /// External blob references of live rows whose object is missing or has another size
    /// (case B4), as `blobs/` keys.
    pub missing_blobs: Vec<String>,
}

impl VerifyReport {
    /// Whether nothing is wrong.
    pub fn is_ok(&self) -> bool {
        self.missing_files.is_empty()
            && self.wrong_size.is_empty()
            && self.invalid_sidecars.is_empty()
            && self.missing_blobs.is_empty()
    }
}

/// What [`crate::Table::repair`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RepairReport {
    /// The version of the repair commit; `None` when nothing was missing.
    pub version: Option<u64>,
    /// Missing data files the commit removed from the table.
    pub files_removed: Vec<String>,
    /// Rows the removed files held, by their statistics.
    pub rows_lost: u64,
}

impl TableInner {
    pub(crate) async fn verify(self: &Arc<Self>) -> Result<VerifyReport> {
        let snap = self.refresh().await?;
        let store = self.db.store.object_store().clone();
        let data = self.layer.data_prefix().clone();
        let listed: Vec<_> = store.list(Some(&data)).try_collect().await?;
        let sizes: HashMap<String, u64> = listed
            .into_iter()
            .map(|m| (m.location.as_ref().to_string(), m.size))
            .collect();
        let mut report = VerifyReport {
            version: snap.version,
            ..Default::default()
        };
        for f in snap.files.iter() {
            report.files_checked += 1;
            let at = data.clone().join(f.path.as_str());
            match sizes.get(at.as_ref()) {
                None => report.missing_files.push(f.path.clone()),
                Some(s) if *s != f.size => report.wrong_size.push(f.path.clone()),
                Some(_) => {}
            }
        }
        // Sidecars: those of live files under a current spec.
        let files: HashMap<String, &crate::log::DataFile> = snap
            .files
            .iter()
            .map(|f| (data_segment(&f.path), f.as_ref()))
            .collect();
        let mut specs = HashMap::new();
        for spec in parse_specs(&snap.meta.configuration)?
            .into_iter()
            .chain(crate::index::key::key_spec(&snap.def))
        {
            if let Ok(kind) = self.db.index.kind(&spec.kind) {
                let hash = spec_hash(kind.as_ref(), &spec);
                specs.insert(spec.name.clone(), (kind.clone(), hash));
            }
        }
        let idx = self.idx_prefix();
        let listed: Vec<_> = store.list(Some(&idx)).try_collect().await?;
        for m in listed {
            let Some((segment, index, hash)) = parse(&idx, &m.location) else {
                continue;
            };
            let (Some(f), Some((kind, current))) = (files.get(&segment), specs.get(&index)) else {
                continue;
            };
            if *current != hash {
                continue;
            }
            report.sidecars_checked += 1;
            let body = store.get(&m.location).await?.bytes().await?;
            let checked = BundleBytes::copy(&body)
                .map_err(|e| e.to_string())
                .and_then(|b| BundleView::parse(b).map_err(|e| e.to_string()))
                .and_then(|v| {
                    v.check(&Expect {
                        kind: kind.name(),
                        kind_format: kind.format_version(),
                        spec_hash: current,
                        source_rows: f.rows(),
                        source_size: f.size,
                    })
                    .and_then(|()| v.verify_sections())
                    .map_err(|e| e.to_string())
                });
            if let Err(why) = checked {
                report
                    .invalid_sidecars
                    .push((m.location.as_ref().to_string(), why));
            }
        }
        self.verify_blobs(&snap, &report.missing_files.clone(), &mut report)
            .await?;
        Ok(report)
    }

    /// Every external blob reference of a live row resolves to an object of its size: one
    /// listing of `blobs/` and a read of each file's blob columns.
    async fn verify_blobs(
        self: &Arc<Self>,
        snap: &crate::snapshot::Snapshot,
        missing_files: &[String],
        report: &mut VerifyReport,
    ) -> Result<()> {
        let schema = snap.schema();
        let cols: Vec<String> = crate::blob::columns(schema).into_iter().collect();
        if cols.is_empty() {
            return Ok(());
        }
        let store = self.db.store.object_store().clone();
        let prefix = self.blob_prefix();
        let listed: Vec<_> = store.list(Some(&prefix)).try_collect().await?;
        let sizes: HashMap<String, u64> = listed
            .into_iter()
            .map(|m| (m.location.as_ref().to_string(), m.size))
            .collect();
        let q = crate::read::FileQuery {
            table_schema: schema.clone(),
            output: cols.clone(),
            needed: crate::schema::project_schema(schema, &cols)?,
            filter: None,
            physical: None,
        };
        for f in snap.files.iter() {
            if missing_files.contains(&f.path) {
                continue;
            }
            let at = self.layer.data_prefix().clone().join(f.path.as_str());
            for b in crate::read::read_file(&self.db.cache, &store, &at, f.size, &q).await? {
                for c in &cols {
                    for r in crate::blob::refs(&b, c)?.into_iter().flatten() {
                        let Some(k) = r.key() else { continue };
                        report.blobs_checked += 1;
                        if !crate::blob::valid_key(k) {
                            report.missing_blobs.push(k.to_string());
                            continue;
                        }
                        let loc = crate::blob::join_key(&prefix, k);
                        if sizes.get(loc.as_ref()) != Some(&r.size()) {
                            report.missing_blobs.push(k.to_string());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn repair(self: &Arc<Self>) -> Result<RepairReport> {
        let v = self.verify().await?;
        let mut report = RepairReport::default();
        if v.missing_files.is_empty() {
            return Ok(report);
        }
        let snap = self.current();
        for f in snap.files.iter() {
            if v.missing_files.contains(&f.path) {
                report.rows_lost += f.rows().unwrap_or(0);
            }
        }
        report.files_removed = v.missing_files.clone();
        let work = ReplaceWork {
            operation: "FSCK",
            remove: v.missing_files,
            batches: Vec::new(),
            ready: Vec::new(),
            tags: Default::default(),
            data_change: true,
            fence: None,
            started: None,
            txns: Vec::new(),
        };
        report.version = Some(self.commit_replace(work).await?);
        Ok(report)
    }
}
