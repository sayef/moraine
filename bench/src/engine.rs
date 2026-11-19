//! The engines under test, behind one set of tracker-shaped operations.
//!
//! The workloads are those of kv0's benchmark gate, so the operations are a tracker's: create a
//! run, log its parameters and metric points, flush, list runs, read a run's parameters and
//! series, read one series, compact, and write and look up records by item id and by run id.
//! Lance and Delta Lake run through kv0's public API with `KV0_ENGINE` choosing the engine.
//! moraine implements the same operations on its own tables ([`crate::moraine_engine`]), in
//! three variants: kv0's table layout with strong consistency (the headline), moraine's own
//! layout, and kv0's layout with eventual consistency. Compaction reports "not yet implemented"
//! for a moraine build without compaction, as in the earliest runs.
//!
//! Reads return values, not counts, so the harness can check them: series as (step, value)
//! pairs, records with their data parsed from JSON as kv0 returns them.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use kv0::tracking::{
    NewRecord, NewRun, RecordQuery, Repo, RepoOptions, RunStatus, StepRange, TrackPoint,
};
use serde_json::{Value, json};

/// moraine's table layout for the tracker tables.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    /// kv0's: metric points carry only a series id (a hash of run, name and context), no run
    /// id column, no clustering.
    Kv0,
    /// moraine's: points carry the run id and are clustered by (run id, series id).
    Own,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Lance,
    Delta,
    Moraine {
        layout: Layout,
        strong: bool,
    },
    /// moraine's queued write mode (D51): appends go to the inbox and a lease-elected
    /// ingestor among the writers commits them. Multi-writer only.
    MoraineQueued,
}

pub const ALL: [Kind; 6] = [
    Kind::Lance,
    Kind::Delta,
    Kind::Moraine {
        layout: Layout::Kv0,
        strong: true,
    },
    Kind::Moraine {
        layout: Layout::Own,
        strong: true,
    },
    Kind::Moraine {
        layout: Layout::Kv0,
        strong: false,
    },
    Kind::MoraineQueued,
];

impl Kind {
    pub fn parse(s: &str) -> Option<Kind> {
        ALL.into_iter().find(|k| k.name() == s)
    }

    pub fn name(self) -> &'static str {
        match self {
            Kind::Lance => "lance",
            Kind::Delta => "delta",
            Kind::Moraine {
                layout: Layout::Kv0,
                strong: true,
            } => "moraine",
            Kind::Moraine {
                layout: Layout::Own,
                strong: true,
            } => "moraine-own",
            Kind::Moraine {
                layout: Layout::Kv0,
                strong: false,
            } => "moraine-eventual",
            Kind::Moraine {
                layout: Layout::Own,
                strong: false,
            } => "moraine-own-eventual",
            Kind::MoraineQueued => "moraine-queued",
        }
    }

    pub fn is_kv0(self) -> bool {
        matches!(self, Kind::Lance | Kind::Delta)
    }
}

/// Error type of the harness: a message, or "not implemented" for moraine's stubs.
#[derive(Debug)]
pub enum BenchError {
    NotImplemented,
    Failed(String),
}

impl<E: std::fmt::Display> From<E> for BenchError {
    fn from(e: E) -> Self {
        BenchError::Failed(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, BenchError>;

/// One open repository on one engine.
pub enum Engine {
    Kv0(Repo),
    Moraine(Box<crate::moraine_engine::Repo>),
}

/// One metric series of a run: points in step order.
pub struct SeriesLog {
    pub name: &'static str,
    pub subset: &'static str,
    pub steps: Vec<i64>,
    pub values: Vec<f64>,
}

/// One run of the logging workload.
pub struct RunLog {
    pub name: String,
    pub experiment: String,
    pub params: Value,
    pub series: Vec<SeriesLog>,
}

/// A run in each engine's native input format, built before the timer starts: kv0's
/// `TrackPoint`s per series with the context already a JSON value. moraine's native input is
/// Arrow batches, and building them is timed as part of its logging calls (the conservative
/// choice: it is work a tracker on moraine would do).
pub enum Prepared {
    Kv0(Vec<(&'static str, Value, Vec<TrackPoint>)>),
    Moraine,
}

/// A record as the engines return it.
pub struct Rec {
    pub item_id: String,
    pub run_id: Option<String>,
    pub data: Value,
}

pub fn context(subset: &str) -> Value {
    json!({ "subset": subset })
}

impl Engine {
    /// Opens `location`; `background_flush` false is the cold start-up handle of the gate
    /// (`flush_interval=None`). `kv0_maintenance` false turns off kv0's background maintenance
    /// (automatic compaction, version cleanup, mark pruning), which otherwise runs inside the
    /// timed phases; the earliest moraine runs had no background maintenance.
    pub async fn open_with(
        kind: Kind,
        location: &str,
        storage: &HashMap<String, String>,
        background_flush: bool,
        kv0_maintenance: bool,
    ) -> Result<Engine> {
        match kind {
            Kind::Lance | Kind::Delta => {
                let mut opts = RepoOptions {
                    storage_options: storage.clone(),
                    ..RepoOptions::default()
                };
                if !background_flush {
                    opts.flush_interval = None;
                }
                if !kv0_maintenance {
                    opts.max_fragments = 0;
                    opts.version_retention = None;
                    opts.mark_prune_interval = Duration::from_secs(10 * 365 * 86_400);
                }
                Ok(Engine::Kv0(Repo::open(location, opts).await?))
            }
            Kind::Moraine { layout, strong } => Ok(Engine::Moraine(Box::new(
                crate::moraine_engine::Repo::open(location, storage, layout, strong, false).await?,
            ))),
            Kind::MoraineQueued => Ok(Engine::Moraine(Box::new(
                crate::moraine_engine::Repo::open(location, storage, Layout::Kv0, true, true)
                    .await?,
            ))),
        }
    }

    /// The run in this engine's native input format (untimed).
    pub fn prepare(&self, run: &RunLog) -> Prepared {
        match self {
            Engine::Kv0(_) => Prepared::Kv0(
                run.series
                    .iter()
                    .map(|s| {
                        let points = s
                            .steps
                            .iter()
                            .zip(&s.values)
                            .map(|(&step, &value)| TrackPoint {
                                value,
                                step: Some(step),
                                epoch: None,
                                timestamp: None,
                            })
                            .collect();
                        (s.name, context(s.subset), points)
                    })
                    .collect(),
            ),
            Engine::Moraine(_) => Prepared::Moraine,
        }
    }

    /// One run of the logging workload: create it, set its parameters, log its points (the
    /// logging calls), then finish it and flush. Returns the two durations separately.
    pub async fn log_run(&self, run: &RunLog, prepared: Prepared) -> Result<(Duration, Duration)> {
        match (self, prepared) {
            (Engine::Kv0(repo), Prepared::Kv0(series)) => {
                let t0 = Instant::now();
                let r = repo.create_run(NewRun {
                    name: Some(run.name.clone()),
                    experiment: Some(run.experiment.clone()),
                    ..Default::default()
                })?;
                r.set_params(&run.params)?;
                for (name, ctx, points) in series {
                    r.track_many(name, Some(ctx), &points)?;
                }
                let calls = t0.elapsed();
                let t1 = Instant::now();
                r.finish(RunStatus::Finished).await?;
                repo.flush().await?;
                Ok((calls, t1.elapsed()))
            }
            (Engine::Moraine(m), _) => m.log_run(run).await,
            _ => Err(BenchError::Failed("prepared for another engine".into())),
        }
    }

    /// Runs as the catalogue lists them: (run id, name).
    pub async fn runs(&self) -> Result<Vec<(String, String)>> {
        match self {
            Engine::Kv0(repo) => Ok(repo
                .runs()
                .await?
                .into_iter()
                .map(|r| (r.run_id, r.name.unwrap_or_default()))
                .collect()),
            Engine::Moraine(m) => m.runs().await,
        }
    }

    /// A run's parameters and series: what a tracker's start-up reads per run. Returns the
    /// number of series.
    pub async fn run_detail(&self, run_id: &str) -> Result<usize> {
        match self {
            Engine::Kv0(repo) => {
                repo.params(run_id).await?;
                Ok(repo.series(run_id).await?.len())
            }
            Engine::Moraine(m) => m.run_detail(run_id).await,
        }
    }

    /// Points of one series in `start..stop` (open ends when `None`), in step order.
    pub async fn read_series(
        &self,
        run_id: &str,
        name: &str,
        subset: &str,
        start: Option<i64>,
        stop: Option<i64>,
    ) -> Result<Vec<(i64, f64)>> {
        match self {
            Engine::Kv0(repo) => {
                let d = repo
                    .read_metric(run_id, name, &context(subset), StepRange { start, stop })
                    .await?;
                Ok(d.steps.into_iter().zip(d.values).collect())
            }
            Engine::Moraine(m) => m.read_series(run_id, name, subset, start, stop).await,
        }
    }

    /// Compaction (kv0's `optimize`).
    pub async fn optimize(&self) -> Result<()> {
        match self {
            Engine::Kv0(repo) => Ok(repo.optimize().await?),
            Engine::Moraine(m) => m.optimize().await,
        }
    }

    /// Writes `rows` (item id, run id, data) to records table `table` in one commit. kv0's
    /// append keeps the newest row per key on read; moraine appends fresh keys and upserts
    /// (`rewrite`) keys that may exist.
    pub async fn write_records(
        &self,
        table: &str,
        rows: Vec<(String, Option<String>, Value)>,
        rewrite: bool,
    ) -> Result<()> {
        match self {
            Engine::Kv0(repo) => {
                let rows = rows
                    .into_iter()
                    .map(|(item_id, run_id, data)| NewRecord {
                        item_id,
                        run_id,
                        data,
                    })
                    .collect();
                repo.append_records(table, rows).await?;
                Ok(())
            }
            Engine::Moraine(m) => m.write_records(table, rows, rewrite).await,
        }
    }

    /// Rows of `table` matching the filters (`None` matches all), every column, data parsed.
    pub async fn records(
        &self,
        table: &str,
        item_id: Option<&str>,
        run_id: Option<&str>,
        run_ids: Option<&[String]>,
    ) -> Result<Vec<Rec>> {
        match self {
            Engine::Kv0(repo) => Ok(repo
                .records(
                    table,
                    RecordQuery {
                        item_id: item_id.map(str::to_string),
                        run_id: run_id.map(str::to_string),
                        run_ids: run_ids.map(<[String]>::to_vec),
                        ..Default::default()
                    },
                )
                .await?
                .into_iter()
                .map(|r| Rec {
                    item_id: r.item_id,
                    run_id: r.run_id,
                    data: r.data,
                })
                .collect()),
            Engine::Moraine(m) => m.records(table, item_id, run_id, run_ids).await,
        }
    }

    /// A record by its primary key (run id, item id), where the engine has a primary-key path
    /// (moraine); kv0 has none.
    pub async fn record_by_key(
        &self,
        table: &str,
        run_id: Option<&str>,
        item_id: &str,
    ) -> Result<Option<Value>> {
        match self {
            Engine::Kv0(_) => Err(BenchError::NotImplemented),
            Engine::Moraine(m) => m.record_by_key(table, run_id, item_id).await,
        }
    }

    /// Rows stored in `table`, without collapsing rows of the same key, where the engine can
    /// tell (moraine); `None` for kv0, whose reads return the newest row per key.
    pub async fn raw_records(&self, table: &str) -> Result<Option<usize>> {
        match self {
            Engine::Kv0(_) => Ok(None),
            Engine::Moraine(m) => Ok(Some(m.raw_records(table).await?)),
        }
    }

    pub async fn close(&self) -> Result<()> {
        match self {
            Engine::Kv0(repo) => Ok(repo.close().await?),
            Engine::Moraine(m) => m.close().await,
        }
    }
}
