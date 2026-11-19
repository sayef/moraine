//! Exactly-once acceptance through the simulation (D32): writes that carry caller transaction ids, as a
//! write-ahead log replays them, with every run checked against the reference model.
//!
//! Each application has a log of writes (appends, upserts, deletes by key and by filter,
//! updates, and writes that change nothing), numbered 1, 2, ... as transaction versions. A
//! replayer sends them in order through `Table::txn` and retries each until it is committed or
//! already applied, as kv0's replay of its write-ahead log does. Three scenarios:
//!
//! - `txn_crash_replay`: each application's process crashes at a random request (the request
//!   may or may not take effect, nothing after it does), restarts as a new process, asks
//!   `last_txn_version` and replays from there, or from an earlier entry, under random faults.
//! - `txn_ambiguous_replay`: most log puts take effect and lose their response; after any
//!   failure the replayer sends the same entry again, from the same process or a new one.
//! - `txn_concurrent_replay`: two processes, sometimes two tasks of one handle (group commit),
//!   replay the same applications' logs at the same time.
//!
//! The committed history is read from the store without moraine: each version's `txn` actions
//! say which entries it holds, its files say which rows it stores. The checker (I11) requires
//! every transaction id at most once and in growing order per application, every "already
//! applied" answer backed by an earlier commit, the `txn` actions to match the entries, and the
//! rows stored at every version to equal the model with each entry applied once (I3).
//!
//! More seeds: `MORAINE_SIM_SEEDS=2000`; one seed: `MORAINE_SIM_SEED=<n>`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::Int64Type;
use async_trait::async_trait;
use futures::stream::BoxStream;
use moraine::expr::{Expr, ScalarValue, col, lit};
use moraine::store::RetryPolicy;
use moraine::{Consistency, Database, Error, Table, TableOptions, TxnWrite, WriteOutcome};
use moraine_testkit::check::{Outcome, Recorder};
use moraine_testkit::datagen;
use moraine_testkit::model::{ModelOp, Row, Txn};
use moraine_testkit::sim::{SimEnv, run_seeds, seeds_from_env};
use moraine_testkit::table_log::{read_log, read_parquet};
use moraine_testkit::{FaultRules, SimRng};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

const ROOT: &str = "db";
const TABLE: &str = "t";
/// Attempts at one entry before a process gives up and restarts.
const ATTEMPTS_PER_LIFE: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SimRow {
    k: String,
    op: String,
    i: i64,
    g: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Pred {
    Keys(Vec<String>),
    Group(i64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Patch {
    op: String,
    i: i64,
}

impl Row for SimRow {
    type Key = String;
    type Pred = Pred;
    type Patch = Patch;
    fn key(&self) -> Option<String> {
        Some(self.k.clone())
    }
    fn matches(&self, p: &Pred) -> bool {
        match p {
            Pred::Keys(ks) => ks.contains(&self.k),
            Pred::Group(g) => self.g == Some(*g),
        }
    }
    fn patched(&self, p: &Patch) -> SimRow {
        SimRow {
            op: p.op.clone(),
            i: p.i,
            ..self.clone()
        }
    }
}

impl Pred {
    fn expr(&self) -> Expr {
        match self {
            Pred::Keys(ks) => col("k").in_list(ks.iter().map(|k| lit(k.as_str())).collect(), false),
            Pred::Group(g) => col("g").eq(lit(*g)),
        }
    }
}

/// One entry of an application's write-ahead log.
#[derive(Clone, Debug)]
enum Entry {
    Append(Vec<SimRow>),
    Upsert(Vec<SimRow>),
    DeleteKeys(Vec<String>),
    DeleteWhere(Pred),
    Update(Pred, Patch),
    /// A write that changes no row; it still records its id.
    Empty,
}

impl Entry {
    fn model(&self) -> ModelOp<SimRow> {
        match self {
            Entry::Append(r) => ModelOp::Append(sorted(r)),
            Entry::Upsert(r) => ModelOp::Upsert(sorted(r)),
            Entry::DeleteKeys(k) => ModelOp::DeleteKeys(k.clone()),
            Entry::DeleteWhere(p) => ModelOp::DeleteWhere(p.clone()),
            Entry::Update(p, x) => ModelOp::Update(p.clone(), x.clone()),
            Entry::Empty => ModelOp::Append(Vec::new()),
        }
    }

    async fn send(&self, w: &TxnWrite) -> moraine::Result<WriteOutcome> {
        match self {
            Entry::Append(r) => w.append(vec![to_batch(r)]).await,
            Entry::Upsert(r) => w.upsert(vec![to_batch(r)]).await,
            Entry::DeleteKeys(ks) => {
                let keys: Vec<Vec<ScalarValue>> = ks
                    .iter()
                    .map(|k| vec![ScalarValue::Utf8(Some(k.clone()))])
                    .collect();
                w.delete_keys(&keys).await
            }
            Entry::DeleteWhere(p) => w.delete(p.expr()).await,
            Entry::Update(p, x) => {
                w.update(
                    p.expr(),
                    vec![
                        ("op".to_string(), lit(x.op.as_str())),
                        ("i".to_string(), lit(x.i)),
                    ],
                )
                .await
            }
            Entry::Empty => w.append(Vec::new()).await,
        }
    }
}

fn sorted(r: &[SimRow]) -> Vec<SimRow> {
    let mut r = r.to_vec();
    r.sort();
    r
}

fn to_batch(rows: &[SimRow]) -> RecordBatch {
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.k.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.op.as_str()),
        )),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.i))),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.g))),
    ];
    RecordBatch::try_new(datagen::row_schema(), cols).expect("batch")
}

fn to_rows(batches: &[RecordBatch]) -> Vec<SimRow> {
    let mut out = Vec::new();
    for b in batches {
        let k = b.column_by_name("k").expect("k").as_string::<i32>();
        let op = b.column_by_name("op").expect("op").as_string::<i32>();
        let i = b
            .column_by_name("i")
            .expect("i")
            .as_primitive::<Int64Type>();
        let g = b
            .column_by_name("g")
            .expect("g")
            .as_primitive::<Int64Type>();
        for r in 0..b.num_rows() {
            out.push(SimRow {
                k: k.value(r).to_string(),
                op: op.value(r).to_string(),
                i: i.value(r),
                g: (!arrow::array::Array::is_null(g, r)).then(|| g.value(r)),
            });
        }
    }
    out
}

/// An application's log of `n` entries. Appended keys are its own; upserts, deletes and
/// updates draw from `shared`, so applications overlap.
fn wal(rng: &mut SimRng, app: &str, n: usize, shared: &[String]) -> Vec<Entry> {
    let pick_keys = |rng: &mut SimRng| -> Vec<String> {
        let mut ks: BTreeSet<String> = BTreeSet::new();
        for _ in 0..rng.range(1, 4) {
            ks.insert(shared[rng.below(shared.len() as u64) as usize].clone());
        }
        ks.into_iter().collect()
    };
    let group = |rng: &mut SimRng| (!rng.chance(0.2)).then(|| rng.below(3) as i64);
    (1..=n)
        .map(|v| {
            let tag = format!("{app}/{v}");
            match rng.below(12) {
                0..=3 => Entry::Append(
                    (0..rng.range(1, 4))
                        .map(|i| SimRow {
                            k: format!("{app}.{v}.{i}"),
                            op: tag.clone(),
                            i: i as i64,
                            g: group(rng),
                        })
                        .collect(),
                ),
                4..=6 => Entry::Upsert(
                    pick_keys(rng)
                        .into_iter()
                        .enumerate()
                        .map(|(i, k)| SimRow {
                            k,
                            op: tag.clone(),
                            i: i as i64,
                            g: group(rng),
                        })
                        .collect(),
                ),
                7 => Entry::DeleteKeys(pick_keys(rng)),
                8 => Entry::DeleteWhere(if rng.chance(0.5) {
                    Pred::Group(rng.below(3) as i64)
                } else {
                    Pred::Keys(pick_keys(rng))
                }),
                9..=10 => Entry::Update(
                    if rng.chance(0.5) {
                        Pred::Keys(pick_keys(rng))
                    } else {
                        Pred::Group(rng.below(3) as i64)
                    },
                    Patch {
                        op: tag,
                        i: 1000 + v as i64,
                    },
                ),
                _ => Entry::Empty,
            }
        })
        .collect()
}

/// A process crash at the store: from the `at`-th request on, nothing reaches the store; the
/// `at`-th itself takes effect or not as `last_lands` says. Every request after it fails, so the
/// process notices, stops, and its answers are not trusted.
#[derive(Debug)]
struct CrashStore {
    inner: Arc<dyn ObjectStore>,
    count: AtomicU64,
    at: u64,
    last_lands: bool,
    crashed: Arc<AtomicBool>,
}

impl std::fmt::Display for CrashStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CrashStore")
    }
}

fn crash_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "crash",
        source: "the process crashed".into(),
    }
}

impl CrashStore {
    /// Whether this request may reach the store, and whether its answer is lost.
    fn admit(&self) -> (bool, bool) {
        let n = self.count.fetch_add(1, Ordering::SeqCst);
        if n < self.at {
            (true, false)
        } else {
            self.crashed.store(true, Ordering::SeqCst);
            (n == self.at && self.last_lands, true)
        }
    }
}

#[async_trait]
impl ObjectStore for CrashStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        match self.admit() {
            (true, false) => self.inner.put_opts(location, payload, opts).await,
            (true, true) => {
                let _ = self.inner.put_opts(location, payload, opts).await;
                Err(crash_error())
            }
            _ => Err(crash_error()),
        }
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        match self.admit() {
            (true, false) => self.inner.put_multipart_opts(location, opts).await,
            _ => Err(crash_error()),
        }
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        match self.admit() {
            (true, false) => self.inner.get_opts(location, options).await,
            _ => Err(crash_error()),
        }
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match self.admit() {
            (true, false) => self.inner.list(prefix),
            _ => Box::pin(futures::stream::once(async { Err(crash_error()) })),
        }
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        match self.admit() {
            (true, false) => self.inner.list_with_delimiter(prefix).await,
            _ => Err(crash_error()),
        }
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        match self.admit() {
            (true, false) => self.inner.copy_opts(from, to, options).await,
            _ => Err(crash_error()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Crash,
    Ambiguous,
    Concurrent,
}

#[derive(Default, Debug)]
struct Totals {
    seeds: u64,
    entries: u64,
    attempts: u64,
    committed: u64,
    already_applied: u64,
    unknown: u64,
    failed: u64,
    crashes: u64,
    lives: u64,
    /// Applications whose whole log was applied by the end of the run.
    complete: u64,
    apps: u64,
    /// Versions holding the ids of two or more applications (group commit).
    grouped_versions: u64,
    faults: u64,
    ambiguous_faults: u64,
}

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// What every replayer of one run shares.
#[derive(Clone)]
struct Ctx {
    rec: Recorder<SimRow>,
    wals: Arc<BTreeMap<String, Vec<Entry>>>,
    attempts: Arc<AtomicU64>,
    totals: Arc<Mutex<Totals>>,
}

/// How a replayer treats answers.
#[derive(Clone, Copy, Default)]
struct Habits {
    /// Chance of sending an entry once more after it was committed (a caller that lost the
    /// answer, or does not trust it).
    resend: f64,
    /// Chance of restarting the process after a failed or unknown attempt, so a new process
    /// sends the entry again.
    restart: f64,
}

/// How a replay ended.
enum Replay {
    /// Every entry is committed or already applied.
    Done,
    /// An entry kept failing, or the process crashed; restart.
    Restart,
}

impl Ctx {
    /// Sends `app`'s entries from index `from` on, in order, each until it is committed or
    /// already applied; `next` is set to the first entry not known to be applied.
    #[allow(clippy::too_many_arguments)]
    async fn replay(
        &self,
        recorder_id: usize,
        table: &Table,
        app: &str,
        from: usize,
        next: &mut usize,
        crashed: Option<&AtomicBool>,
        rng: &mut SimRng,
        habits: Habits,
    ) -> Replay {
        let wal = &self.wals[app];
        *next = from;
        while *next < wal.len() {
            let entry = &wal[*next];
            let txn = Txn::new(app, *next as i64 + 1);
            let mut resolved = false;
            let mut resends = 0;
            let mut tries = 0;
            while tries < ATTEMPTS_PER_LIFE {
                tries += 1;
                let n = self.attempts.fetch_add(1, Ordering::SeqCst);
                let op = format!("{app}/{}#{n}", txn.version);
                self.rec
                    .intend_txn(recorder_id, &op, entry.model(), txn.clone());
                let r = entry.send(&table.txn(app, txn.version)).await;
                let dead = crashed.is_some_and(|c| c.load(Ordering::SeqCst));
                let outcome = match &r {
                    _ if dead => Outcome::Unknown,
                    Ok(WriteOutcome::Committed(c)) => Outcome::Acked { version: c.version },
                    Ok(WriteOutcome::AlreadyApplied { version, .. }) => {
                        Outcome::AlreadyApplied { version: *version }
                    }
                    Ok(other) => panic!("an outcome this simulation does not know: {other:?}"),
                    Err(Error::OutcomeUnknown { .. }) => Outcome::Unknown,
                    Err(e) => Outcome::Failed(e.to_string()),
                };
                {
                    let mut t = self.totals.lock().unwrap();
                    t.attempts += 1;
                    match &outcome {
                        Outcome::Acked { .. } => t.committed += 1,
                        Outcome::AlreadyApplied { .. } => t.already_applied += 1,
                        Outcome::Unknown => t.unknown += 1,
                        Outcome::Failed(_) => t.failed += 1,
                    }
                }
                let ok = matches!(
                    outcome,
                    Outcome::Acked { .. } | Outcome::AlreadyApplied { .. }
                );
                self.rec.outcome(recorder_id, &op, outcome);
                if dead {
                    return Replay::Restart;
                }
                if ok {
                    resolved = true;
                    if resends < 2 && rng.chance(habits.resend) {
                        resends += 1;
                        tries -= 1;
                        continue;
                    }
                    break;
                }
                if !resolved && rng.chance(habits.restart) {
                    return Replay::Restart;
                }
            }
            if !resolved {
                return Replay::Restart;
            }
            *next += 1;
        }
        Replay::Done
    }
}

fn options() -> TableOptions {
    TableOptions::default()
        .primary_key(["k"])
        .enforce_primary_key(false)
        .cluster_by(["g"])
        .page_row_limit(2)
}

/// A process of client `client`: its `life`-th start draws fresh randomness (writer id, file
/// names), and its store may crash.
fn open_process(
    env: &SimEnv,
    client: usize,
    life: u64,
    rules: &FaultRules,
    crash: Option<(u64, bool, Arc<AtomicBool>)>,
    linger: Duration,
) -> impl Future<Output = moraine::Result<Database>> + Send + 'static {
    let clock = moraine_testkit::ClientClock::new(
        env.clock.clone(),
        Some(env.sched.clone()),
        client,
        // Hashed, not `seed ^ life * GOLDEN`: SplitMix64 steps its state by GOLDEN, so such
        // seeds can give two processes the same stream shifted by one draw, and with it the
        // same data file names.
        moraine::testing::splitmix64(
            env.seed ^ moraine::testing::splitmix64((client as u64) << 32 | life),
        ),
    );
    let mut store = env.chaos_store(client, rules.clone());
    if let Some((at, last_lands, crashed)) = crash {
        store = Arc::new(CrashStore {
            inner: store,
            count: AtomicU64::new(0),
            at,
            last_lands,
            crashed,
        });
    }
    Database::builder_for_store(store, ROOT)
        .clock(Arc::new(clock))
        .io_concurrency(1)
        .consistency(Consistency::Strong)
        .group_commit_linger(linger)
        .request_retry(RetryPolicy::REQUEST)
        .open()
}

async fn open_table(db: impl Future<Output = moraine::Result<Database>>) -> Option<Table> {
    let db = db.await.ok()?;
    for _ in 0..5 {
        if let Ok(t) = db.open_table(TABLE).await {
            return Some(t);
        }
    }
    None
}

async fn scenario(mut env: SimEnv, mode: Mode, totals: Arc<Mutex<Totals>>) -> Result<(), String> {
    let apps_n = match mode {
        Mode::Concurrent => env.rng.range(1, 3),
        _ => env.rng.range(2, 5),
    } as usize;
    let shared: Vec<String> = (0..env.rng.range(2, 6))
        .map(|n| format!("{}#{n}", datagen::hostile_string(&mut env.rng)))
        .collect();
    let mut wals = BTreeMap::new();
    for a in 0..apps_n {
        let app = format!("app{a}");
        let n = env.rng.range(3, 8) as usize;
        wals.insert(app.clone(), wal(&mut env.rng, &app, n, &shared));
    }
    let apps: Vec<String> = wals.keys().cloned().collect();
    let rules = match mode {
        Mode::Crash => {
            let mut r = FaultRules::random(&mut env.rng);
            r.ambiguous_commits = env.rng.below(200) as f64 / 1000.0;
            r
        }
        // Lookups after an ambiguous put fail often enough that some writes end "outcome
        // unknown" and are sent again.
        Mode::Ambiguous => FaultRules {
            ambiguous_commits: 0.3 + env.rng.below(600) as f64 / 1000.0,
            fail_after: env.rng.below(50) as f64 / 1000.0,
            fail_before: env.rng.below(400) as f64 / 1000.0,
            ..Default::default()
        },
        Mode::Concurrent => {
            if env.rng.chance(0.5) {
                FaultRules::random(&mut env.rng)
            } else {
                FaultRules::default()
            }
        }
    };
    let faultless = matches!(mode, Mode::Concurrent)
        && rules.fail_before == 0.0
        && rules.fail_after == 0.0
        && rules.throttle == 0.0
        && rules.stall == 0.0
        && rules.truncate == 0.0
        && rules.overlap_409 == 0.0
        && rules.cut_listing == 0.0;
    let ctx = Ctx {
        rec: Recorder::default(),
        wals: Arc::new(wals),
        attempts: Arc::default(),
        totals: totals.clone(),
    };
    {
        let mut t = totals.lock().unwrap();
        t.seeds += 1;
        t.apps += apps_n as u64;
        t.entries += ctx.wals.values().map(|w| w.len() as u64).sum::<u64>();
    }

    let setup = open_process(&env, 0, 999, &FaultRules::default(), None, Duration::ZERO);
    env.run(vec![Box::pin(async move {
        let db = setup.await.expect("open");
        db.create_table(TABLE, datagen::row_schema(), options())
            .await
            .expect("create");
    }) as ClientFuture])
        .await
        .map_err(|e| format!("setup: {e:?}"))?;

    let mut futs: Vec<ClientFuture> = Vec::new();
    match mode {
        Mode::Crash | Mode::Ambiguous => {
            for (c, app) in apps.iter().enumerate() {
                let mut rng = env.rng.fork(c as u64 + 1);
                let lives = if mode == Mode::Crash { 6 } else { 8 };
                let mut procs = Vec::new();
                for life in 0..lives {
                    let crashed = Arc::new(AtomicBool::new(false));
                    let crash = (mode == Mode::Crash && rng.chance(0.6))
                        .then(|| (rng.range(2, 60), rng.chance(0.5), crashed.clone()));
                    procs.push((
                        open_process(&env, c, life, &rules, crash, Duration::ZERO),
                        crashed,
                    ));
                }
                let (ctx, app) = (ctx.clone(), app.clone());
                futs.push(Box::pin(async move {
                    // Where the previous process stopped (Ambiguous: the entry to resend).
                    let mut next = 0usize;
                    for (life, (db, crashed)) in procs.into_iter().enumerate() {
                        ctx.totals.lock().unwrap().lives += 1;
                        let id = c * 100 + life;
                        let Some(table) = open_table(db).await else {
                            if crashed.load(Ordering::SeqCst) {
                                ctx.totals.lock().unwrap().crashes += 1;
                            }
                            continue;
                        };
                        let from = match mode {
                            Mode::Crash => {
                                let Ok(last) = table.last_txn_version(&app).await else {
                                    if crashed.load(Ordering::SeqCst) {
                                        ctx.totals.lock().unwrap().crashes += 1;
                                    }
                                    continue;
                                };
                                let resume = last.unwrap_or(0) as usize;
                                // kv0 resumes after the last applied entry; a careless replay
                                // starts earlier and must be told "already applied".
                                if rng.chance(0.5) {
                                    resume
                                } else {
                                    rng.below(resume as u64 + 1) as usize
                                }
                            }
                            _ => next,
                        };
                        let habits = match mode {
                            Mode::Ambiguous => Habits {
                                resend: 0.4,
                                restart: 0.5,
                            },
                            _ => Habits::default(),
                        };
                        let r = ctx
                            .replay(
                                id,
                                &table,
                                &app,
                                from,
                                &mut next,
                                Some(&crashed),
                                &mut rng,
                                habits,
                            )
                            .await;
                        if crashed.load(Ordering::SeqCst) {
                            ctx.totals.lock().unwrap().crashes += 1;
                        }
                        if let Replay::Done = r {
                            break;
                        }
                    }
                }));
            }
        }
        Mode::Concurrent => {
            // Two processes; each runs one replayer per application, and sometimes a second one
            // on the same handle.
            for c in 0..2usize {
                let twice = env.rng.chance(0.5);
                let linger = Duration::from_millis(env.rng.range(0, 30));
                let slot: Arc<Mutex<Option<Option<Table>>>> = Arc::default();
                let tasks = if twice { 2 } else { 1 };
                for t in 0..tasks {
                    let db =
                        (t == 0).then(|| open_process(&env, c * 10 + t, 0, &rules, None, linger));
                    let clock = env.client_clock(c * 10 + t);
                    let (ctx, slot, apps) = (ctx.clone(), slot.clone(), apps.clone());
                    let mut rng = env.rng.fork((c * 10 + t) as u64 + 1);
                    futs.push(Box::pin(async move {
                        use moraine::clock::Clock;
                        let table = match db {
                            Some(db) => {
                                let t = open_table(db).await;
                                *slot.lock().unwrap() = Some(t.clone());
                                t
                            }
                            None => loop {
                                let got = slot.lock().unwrap().clone();
                                if let Some(t) = got {
                                    break t;
                                }
                                clock.sleep(Duration::from_millis(5)).await;
                            },
                        };
                        let Some(table) = table else { return };
                        // Applications in a random order, each replayed to its end.
                        let mut order = apps.clone();
                        for i in (1..order.len()).rev() {
                            order.swap(i, rng.below(i as u64 + 1) as usize);
                        }
                        for (k, app) in order.iter().enumerate() {
                            let mut next = 0;
                            for round in 0..4 {
                                let id = (c * 10 + t) * 100 + k * 10 + round;
                                let from = if round == 0 && rng.chance(0.5) {
                                    0
                                } else {
                                    next
                                };
                                let habits = Habits {
                                    resend: 0.2,
                                    restart: 0.0,
                                };
                                if let Replay::Done = ctx
                                    .replay(
                                        id, &table, app, from, &mut next, None, &mut rng, habits,
                                    )
                                    .await
                                {
                                    break;
                                }
                            }
                        }
                    }));
                }
            }
        }
    }
    env.run(futs)
        .await
        .map_err(|e| format!("run did not finish: {e:?}"))?;

    // A fresh process without faults reads the final version and every application's id.
    let fresh = open_process(&env, 99, 0, &FaultRules::default(), None, Duration::ZERO);
    let rec = ctx.rec.clone();
    let reported: Arc<Mutex<BTreeMap<String, Option<i64>>>> = Arc::default();
    let (rep, apps2) = (reported.clone(), apps.clone());
    env.run(vec![Box::pin(async move {
        let db = fresh.await.expect("open");
        let t = db.open_table(TABLE).await.expect("open table");
        let s = t.snapshot().await.expect("snapshot");
        let b = s.scan().collect().await.expect("scan");
        rec.read(9999, s.version(), to_rows(&b));
        for app in apps2 {
            let v = s.last_txn_version(&app);
            rep.lock().unwrap().insert(app, v);
        }
    }) as ClientFuture])
        .await
        .map_err(|e| format!("final read did not finish: {e:?}"))?;

    let (log, physical, actions, grouped) = committed_history(&env, &ctx.rec, &apps)?;
    // The newest id per application, as the log says and as moraine reports it.
    let mut newest: BTreeMap<String, i64> = BTreeMap::new();
    for set in actions.values() {
        for t in set {
            let e = newest.entry(t.app_id.clone()).or_insert(t.version);
            *e = (*e).max(t.version);
        }
    }
    for (app, v) in reported.lock().unwrap().iter() {
        if *v != newest.get(app).copied() {
            return Err(format!(
                "last_txn_version({app}) is {v:?}, the log's newest is {:?}",
                newest.get(app)
            ));
        }
    }
    let complete = apps
        .iter()
        .filter(|a| newest.get(*a).copied() == Some(ctx.wals[*a].len() as i64))
        .count();
    if faultless && complete != apps.len() {
        return Err(format!(
            "without faults every log must be applied; {complete} of {} were",
            apps.len()
        ));
    }
    {
        let mut t = totals.lock().unwrap();
        t.complete += complete as u64;
        t.grouped_versions += grouped;
        t.faults += env.faults.total();
        t.ambiguous_faults += env.faults.ambiguous();
    }
    ctx.rec
        .history(log)
        .with_physical(physical)
        .with_txn_actions(actions)
        .check(false)
        .map_err(|v| v.to_string())
}

type LogLine = (u64, String, ModelOp<SimRow>);
type Committed = (
    Vec<LogLine>,
    BTreeMap<u64, Vec<SimRow>>,
    BTreeMap<u64, BTreeSet<Txn>>,
    u64,
);

/// Reads the committed history from the store: each version's caller `txn` actions place the
/// entries it holds (an attempt acknowledged with that version if there is one, else one whose
/// outcome is unknown, else the first, which the checker then rejects), and its files give the
/// rows stored at every version.
fn committed_history(
    env: &SimEnv,
    rec: &Recorder<SimRow>,
    apps: &[String],
) -> Result<Committed, String> {
    let root = format!("{ROOT}/{TABLE}/data");
    let intended = rec.intended();
    let outcomes = rec.outcomes();
    let mut attempts: BTreeMap<Txn, Vec<String>> = BTreeMap::new();
    for (op, t) in rec.txns() {
        attempts.entry(t).or_default().push(op);
    }
    let mut files: BTreeMap<String, Vec<SimRow>> = BTreeMap::new();
    let mut log: Vec<LogLine> = Vec::new();
    let mut physical: BTreeMap<u64, Vec<SimRow>> = BTreeMap::new();
    let mut actions: BTreeMap<u64, BTreeSet<Txn>> = BTreeMap::new();
    let mut placed: BTreeSet<String> = BTreeSet::new();
    let mut grouped = 0;
    for e in read_log(&env.store, &root) {
        for p in &e.removes {
            if files.remove(p).is_none() {
                return Err(format!(
                    "version {} removes {p}, which is not live",
                    e.version
                ));
            }
        }
        for p in &e.adds {
            let rows = to_rows(&read_parquet(&env.store, &format!("{root}/{p}")));
            if files.insert(p.clone(), rows).is_some() {
                return Err(format!("version {} adds {p} twice", e.version));
            }
        }
        physical.insert(e.version, files.values().flatten().cloned().collect());
        let callers: BTreeSet<Txn> = e
            .txns
            .iter()
            .filter(|(a, _)| apps.contains(a))
            .map(|(a, v)| Txn::new(a.clone(), *v))
            .collect();
        if callers.len() > 1 {
            grouped += 1;
        }
        for txn in &callers {
            let Some(ops) = attempts.get(txn) else {
                return Err(format!(
                    "version {} holds {txn}, which no attempt carried",
                    e.version
                ));
            };
            let acked = Outcome::Acked { version: e.version };
            let op = ops
                .iter()
                .find(|op| outcomes.get(*op) == Some(&acked))
                .or_else(|| {
                    ops.iter().find(|op| {
                        outcomes.get(*op).is_none_or(|o| *o == Outcome::Unknown)
                            && !placed.contains(*op)
                    })
                })
                .unwrap_or(&ops[0])
                .clone();
            placed.insert(op.clone());
            let what = intended
                .get(&op)
                .cloned()
                .ok_or_else(|| format!("{op} not intended"))?;
            log.push((e.version, op, what));
        }
        if !callers.is_empty() {
            actions.insert(e.version, callers);
        }
    }
    Ok((log, physical, actions, grouped))
}

fn run(mode: Mode, seeds: std::ops::Range<u64>) -> Totals {
    let totals = Arc::new(Mutex::new(Totals::default()));
    let t = totals.clone();
    if let Err(f) = run_seeds(seeds.clone(), move |env| scenario(env, mode, t.clone())) {
        panic!("{f}");
    }
    let t = std::mem::take(&mut *totals.lock().unwrap());
    println!(
        "RESULT txn_sim {mode:?} seeds={}..{} {t:?}",
        seeds.start, seeds.end
    );
    t
}

/// Processes crash at random requests and restart; replays resume from `last_txn_version` or
/// earlier. No entry applies twice.
#[test]
fn txn_crash_replay() {
    let t = run(Mode::Crash, seeds_from_env(150));
    assert!(t.crashes > 0, "no process crashed");
    assert!(t.already_applied > 0, "no replay was told already applied");
    assert!(t.complete > 0);
}

/// Log puts take effect and lose their response; the same entry is sent again, from the same
/// process or a new one, and is applied once.
#[test]
fn txn_ambiguous_replay() {
    let t = run(Mode::Ambiguous, seeds_from_env(150));
    assert!(t.ambiguous_faults > 0);
    assert!(t.already_applied > 0, "no replay was told already applied");
    assert!(t.complete > 0);
}

/// Two processes (and two tasks of one handle) replay the same logs at once.
#[test]
fn txn_concurrent_replay() {
    let t = run(Mode::Concurrent, seeds_from_env(150));
    assert!(t.already_applied > 0);
    assert!(
        t.grouped_versions > 0,
        "no commit held two applications' ids"
    );
}
