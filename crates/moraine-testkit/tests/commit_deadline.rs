//! Cases C6 and C20: a writer that stalls past the commit deadline
//! between uploading its files and committing starts again with fresh uploads, and a write that
//! cannot finish within the deadline even then fails with `CommitDeadlineExceeded` and leaves
//! nothing visible.
//!
//! The stall is a failpoint callback that moves the simulated clock past the deadline right
//! after the upload. Failpoints are global to the process, so this file is its own test binary
//! and its tests run one after the other.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use moraine::store::RetryPolicy;
use moraine::{Consistency, Database, Error, TableOptions};
use moraine_testkit::datagen;
use moraine_testkit::sim::{SimEnv, run_seed};
use moraine_testkit::table_log::read_log;
use moraine_testkit::{FaultRules, SimRng};

type Stalled = (
    Result<u64, String>,
    u32,
    Vec<moraine_testkit::table_log::CommittedEntry>,
    usize,
);

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Serialises the tests of this binary: failpoints are process-wide.
static LOCK: Mutex<()> = Mutex::new(());

const GRACE: Duration = Duration::from_secs(3600);

fn open(env: &SimEnv) -> impl Future<Output = moraine::Result<Database>> + Send + 'static {
    Database::builder_for_store(env.chaos_store(0, FaultRules::default()), "db")
        .clock(env.client_clock(0))
        .io_concurrency(1)
        .consistency(Consistency::Strong)
        .request_retry(RetryPolicy::REQUEST)
        .open()
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Append,
    Upsert,
    Delete,
}

impl Op {
    fn point(self) -> &'static str {
        match self {
            Op::Append => "moraine::append::after_upload",
            Op::Upsert | Op::Delete => "moraine::rewrite::after_upload",
        }
    }
}

/// Runs `op` with the clock jumping past the commit deadline after the first `stalls` uploads.
/// Returns what the write returned, how many uploads happened, the committed log, and the data
/// files no log entry references.
fn stalled(seed: u64, op: Op, stalls: u32) -> Stalled {
    let _g = LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let out: Arc<Mutex<Option<Stalled>>> = Arc::default();
    let o = out.clone();
    let r = run_seed(seed, move |env| async move {
        let uploads = Arc::new(AtomicU32::new(0));
        let db = open(&env);
        let (clock, u) = (env.clock.clone(), uploads.clone());
        let keyed = TableOptions::default()
            .primary_key(["k"])
            .grace_period(GRACE);
        let result: Arc<Mutex<Option<Result<u64, String>>>> = Arc::default();
        let res = result.clone();
        let fut: ClientFuture = Box::pin(async move {
            let db = db.await.expect("db");
            let t = db
                .create_table("t", datagen::row_schema(), keyed)
                .await
                .expect("create");
            let mut rng = SimRng::new(seed);
            let base = datagen::rows(&mut rng, "base", 4, None, 2);
            t.upsert(vec![base.clone()]).await.expect("base rows");
            fail::cfg_callback(op.point(), move || {
                if u.fetch_add(1, Ordering::SeqCst) < stalls {
                    clock.advance(GRACE / 4 + Duration::from_secs(1));
                }
            })
            .expect("failpoint");
            let r = match op {
                Op::Append => {
                    t.append_unchecked(vec![datagen::rows(&mut rng, "new", 3, None, 2)])
                        .await
                }
                Op::Upsert => t.upsert(vec![base.slice(0, 2)]).await,
                Op::Delete => {
                    use moraine::expr::{col, lit};
                    let k = base
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .expect("k")
                        .value(0)
                        .to_string();
                    t.delete(col("k").eq(lit(k))).await
                }
            };
            fail::remove(op.point());
            *res.lock().unwrap() = Some(r.map(|c| c.version).map_err(|e| match e {
                Error::CommitDeadlineExceeded { .. } => "CommitDeadlineExceeded".to_string(),
                other => other.to_string(),
            }));
        });
        env.run(vec![fut]).await.map_err(|e| format!("{e:?}"))?;
        let r = result.lock().unwrap().take().expect("finished");
        let log = read_log(&env.store, "db/t/data");
        let referenced: Vec<&String> = log.iter().flat_map(|e| &e.adds).collect();
        let orphans = env
            .store
            .snapshot()
            .keys()
            .filter_map(|k| k.strip_prefix("db/t/data/"))
            .filter(|k| k.ends_with(".parquet") && !referenced.iter().any(|r| r == k))
            .count();
        *o.lock().unwrap() = Some((r, uploads.load(Ordering::SeqCst), log, orphans));
        Ok(())
    });
    if let Err(f) = r {
        panic!("{f}");
    }
    out.lock().unwrap().take().expect("ran")
}

/// Case C6: one stall past the deadline. The write starts again from its commit clock with
/// fresh uploads and commits; the committed entry references only the second upload's files,
/// and the first upload's are deleted once it has (D65).
#[test]
fn slow_writer_restarts_with_fresh_uploads() {
    for (seed, op) in [(1, Op::Append), (2, Op::Upsert), (3, Op::Delete)] {
        let (r, uploads, log, orphans) = stalled(seed, op, 1);
        assert_eq!(r, Ok(2), "{op:?}: the restarted write commits as version 2");
        assert_eq!(uploads, 2, "{op:?}: uploaded twice");
        assert_eq!(log.len(), 3, "{op:?}: one commit after the base rows");
        assert!(
            !log[2].adds.is_empty() || op_is_delete(op),
            "{op:?}: adds files"
        );
        assert_eq!(orphans, 0, "{op:?}: the stalled upload is left behind");
    }
}

/// Case C20: every attempt stalls past the deadline. The write fails with
/// `CommitDeadlineExceeded`, never commits late, and leaves nothing visible (I2). Its uploads
/// are deleted (D65).
#[test]
fn write_too_slow_for_the_deadline_never_commits() {
    for (seed, op) in [(4, Op::Append), (5, Op::Upsert), (6, Op::Delete)] {
        let (r, uploads, log, orphans) = stalled(seed, op, u32::MAX);
        assert_eq!(r, Err("CommitDeadlineExceeded".to_string()), "{op:?}");
        assert!(uploads >= 2, "{op:?}: restarted once before giving up");
        assert_eq!(
            log.len(),
            2,
            "{op:?}: nothing committed after the base rows"
        );
        assert_eq!(orphans, 0, "{op:?}: uploads left behind");
    }
}

fn op_is_delete(op: Op) -> bool {
    matches!(op, Op::Delete)
}
