//! Upsert, delete and update acceptance through the simulation: upsert, delete by
//! key and by predicate, update, and add_column, from several clients and several tasks per
//! handle (group commit), with random faults, every run checked against the reference model.
//!
//! Named scenarios (docs/design/test-ids.md): 2 `upsert_same_key_race`, 15 `schema_add_column_race`.
//! `mutations_random` mixes every operation with random faults (I1 to I5, C2, C3, C9, C10, C16,
//! C19, D2); `mutations_no_faults` runs the same mix where every write must succeed.
//!
//! The committed history is read from the store without moraine: the rows each version stores
//! (I3 against the model at every version, not only at the versions clients read), and which
//! operation each version holds. Rows name the operation that wrote them (`op`), and so do
//! updated rows and added columns; deletes and writes that changed nothing leave no trace, so
//! they are placed by the version they were acknowledged with, and a version is accepted only
//! if some order of its operations explains what it stores (serial equivalence, I3).
//!
//! More seeds: `MORAINE_SIM_SEEDS=100000`; one seed: `MORAINE_SIM_SEED=<n>`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use moraine::expr::{Expr, ScalarValue, col, lit};
use moraine::store::RetryPolicy;
use moraine::{Consistency, Database, DuplicatePolicy, Error, Table, TableOptions};
use moraine_testkit::check::{Outcome, Recorder};
use moraine_testkit::datagen;
use moraine_testkit::model::{ModelOp, Row, TableModel};
use moraine_testkit::sim::{SimEnv, run_seeds, seeds_from_env};
use moraine_testkit::table_log::{read_log, read_parquet};
use moraine_testkit::{FaultRules, SimRng};

const ROOT: &str = "db";
const TABLE: &str = "t";

/// Seeds that failed during development, with their scenario; they run on every test run.
const REGRESSION_SEEDS: &[(u64, Mode)] = &[
    (363, Mode::Random),
    (30504, Mode::Random),
    (81876, Mode::Random),
    (96409, Mode::Random),
    (13904, Mode::NoFaults),
    (36333, Mode::NoFaults),
    (72400, Mode::NoFaults),
    (76811, Mode::NoFaults),
];

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SimRow {
    k: String,
    op: String,
    i: i64,
    g: Option<i64>,
    /// Non-null values of columns added by `add_column`.
    extra: BTreeMap<String, i64>,
    keyed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Pred {
    Keys(Vec<String>),
    Group(Option<i64>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Patch {
    op: String,
    i: i64,
    /// `Some(g)` sets the group (a cluster-key column, so rows move in the sort order).
    g: Option<Option<i64>>,
}

impl Row for SimRow {
    type Key = String;
    type Pred = Pred;
    type Patch = Patch;
    fn key(&self) -> Option<String> {
        self.keyed.then(|| self.k.clone())
    }
    fn matches(&self, p: &Pred) -> bool {
        match p {
            Pred::Keys(ks) => ks.contains(&self.k),
            Pred::Group(g) => self.g == *g,
        }
    }
    fn patched(&self, p: &Patch) -> SimRow {
        let mut r = self.clone();
        r.op = p.op.clone();
        r.i = p.i;
        if let Some(g) = p.g {
            r.g = g;
        }
        r
    }
}

impl Pred {
    fn expr(&self) -> Expr {
        match self {
            Pred::Keys(ks) => col("k").in_list(ks.iter().map(|k| lit(k.as_str())).collect(), false),
            Pred::Group(Some(g)) => col("g").eq(lit(*g)),
            Pred::Group(None) => col("g").is_null(),
        }
    }
}

impl Patch {
    fn assignments(&self) -> Vec<(String, Expr)> {
        let mut a = vec![
            ("op".to_string(), lit(self.op.as_str())),
            ("i".to_string(), lit(self.i)),
        ];
        if let Some(g) = self.g {
            a.push(("g".to_string(), lit(ScalarValue::Int64(g))));
        }
        a
    }
}

fn to_rows(batches: &[RecordBatch], keyed: bool) -> Vec<SimRow> {
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
        let schema = b.schema();
        let extras: Vec<(String, &arrow::array::PrimitiveArray<Int64Type>)> = schema
            .fields()
            .iter()
            .filter(|f| f.name().starts_with('x'))
            .map(|f| {
                (
                    f.name().clone(),
                    b.column_by_name(f.name())
                        .expect("x")
                        .as_primitive::<Int64Type>(),
                )
            })
            .collect();
        for r in 0..b.num_rows() {
            out.push(SimRow {
                k: k.value(r).to_string(),
                op: op.value(r).to_string(),
                i: i.value(r),
                g: (!g.is_null(r)).then(|| g.value(r)),
                extra: extras
                    .iter()
                    .filter(|(_, a)| !a.is_null(r))
                    .map(|(n, a)| (n.clone(), a.value(r)))
                    .collect(),
                keyed,
            });
        }
    }
    out
}

/// A batch of `rows` with the base columns and the added columns in `extra_cols`.
fn to_batch(rows: &[SimRow], extra_cols: &[String]) -> RecordBatch {
    let mut fields: Vec<Field> = datagen::row_schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.k.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.op.as_str()),
        )),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.i))),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.g))),
    ];
    for c in extra_cols {
        fields.push(Field::new(c, DataType::Int64, true));
        cols.push(Arc::new(Int64Array::from_iter(
            rows.iter().map(|r| r.extra.get(c).copied()),
        )));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).expect("batch")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Every operation, random faults.
    Random,
    /// Every operation, no faults: every write must succeed or fail as the test expects.
    NoFaults,
    /// Scenario 2: two writers upsert the same new key, round after round.
    SameKey,
    /// Scenario 15: appends with the old schema while columns are added.
    AddColumnRace,
}

#[derive(Default, Debug)]
struct Totals {
    seeds: u64,
    ops: u64,
    acked: u64,
    failed: u64,
    unknown: u64,
    versions: u64,
    upserts: u64,
    deletes: u64,
    updates: u64,
    add_columns: u64,
    appends: u64,
    grouped_versions: u64,
    rewritten_files: u64,
    reads: u64,
    requests: u64,
    faults: u64,
    ambiguous_faults: u64,
}

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

fn options(enforce: bool) -> TableOptions {
    TableOptions::default()
        .primary_key(["k"])
        .enforce_primary_key(enforce)
        .cluster_by(["g"])
        .page_row_limit(2)
}

fn open_db(
    env: &SimEnv,
    client: usize,
    rules: &FaultRules,
    consistency: Consistency,
    small: bool,
) -> impl Future<Output = moraine::Result<Database>> + Send + 'static {
    Database::builder_for_store(env.chaos_store(client, rules.clone()), ROOT)
        .clock(env.client_clock(client))
        .io_concurrency(1)
        .consistency(consistency)
        .small_table_bytes(if small { 64 << 20 } else { 0 })
        .request_retry(RetryPolicy::REQUEST)
        .open()
}

/// The operations a task can run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Upsert,
    DeleteKeys,
    DeleteWhere,
    Update,
    AddColumn,
    Append,
}

fn pick(rng: &mut SimRng, mode: Mode, client: usize, j: usize) -> Kind {
    match mode {
        Mode::SameKey => Kind::Upsert,
        Mode::AddColumnRace => {
            if client == 0 && j % 2 == 1 {
                Kind::AddColumn
            } else {
                Kind::Append
            }
        }
        Mode::Random | Mode::NoFaults => match rng.below(20) {
            0..=7 => Kind::Upsert,
            8..=9 => Kind::DeleteKeys,
            10..=11 => Kind::DeleteWhere,
            12..=14 => Kind::Update,
            15 => Kind::AddColumn,
            _ => Kind::Append,
        },
    }
}

/// The added columns a handle knows of, in schema order.
fn extra_columns(t: &Table) -> Vec<String> {
    t.schema()
        .fields()
        .iter()
        .filter(|f| f.name().starts_with('x'))
        .map(|f| f.name().clone())
        .collect()
}

async fn scenario(mut env: SimEnv, mode: Mode, totals: Arc<Mutex<Totals>>) -> Result<(), String> {
    let (clients, tasks_per_client, ops_per_task) = match mode {
        Mode::SameKey => (2, 1, env.rng.range(2, 6) as usize),
        Mode::AddColumnRace => (
            env.rng.range(2, 5) as usize,
            env.rng.range(1, 3) as usize,
            4,
        ),
        _ => (
            env.rng.range(2, 5) as usize,
            env.rng.range(1, 4) as usize,
            env.rng.range(1, 6) as usize,
        ),
    };
    // Scenario 15 runs on a table whose key is not enforced, so it can use plain appends.
    let enforce = match mode {
        Mode::AddColumnRace => false,
        Mode::SameKey => true,
        _ => env.rng.chance(0.75),
    };
    let rules = match mode {
        Mode::Random => FaultRules::random(&mut env.rng),
        _ => FaultRules::default(),
    };
    // A small key space so operations overlap; keys are hostile strings.
    let mut keys: BTreeSet<String> = BTreeSet::new();
    let key_space = env.rng.range(2, 9);
    for n in 0..key_space {
        keys.insert(format!("{}#{n}", datagen::hostile_string(&mut env.rng)));
    }
    let keys: Arc<Vec<String>> = Arc::new(keys.into_iter().collect());

    let recorder = Recorder::<SimRow>::default();
    let violations: Arc<Mutex<Vec<String>>> = Arc::default();
    let reads: Arc<Mutex<u64>> = Arc::default();
    // The intended operations, by id, with their kind.
    let kinds: Arc<Mutex<BTreeMap<String, Kind>>> = Arc::default();

    // Client 0 creates the table first so every client opens the same one.
    let setup = open_db(&env, 0, &FaultRules::default(), Consistency::Strong, false);
    env.run(vec![Box::pin(async move {
        let db = setup.await.expect("open");
        db.create_table(TABLE, datagen::row_schema(), options(enforce))
            .await
            .expect("create");
    }) as ClientFuture])
        .await
        .map_err(|e| format!("setup: {e:?}"))?;

    let mut futs: Vec<ClientFuture> = Vec::new();
    for c in 0..clients {
        let consistency = match (mode, env.rng.below(3)) {
            (Mode::Random, 0) => Consistency::Strong,
            (Mode::Random, 1) => Consistency::Manual,
            _ => Consistency::Eventual {
                max_staleness: Duration::from_millis(env.rng.range(0, 200)),
            },
        };
        let small = env.rng.chance(0.5);
        let slot: Arc<Mutex<Option<Option<Table>>>> = Arc::default();
        for t in 0..tasks_per_client {
            let task = c * 10 + t;
            let mut rng = env.rng.fork(task as u64 + 1);
            let (rec, viol, slot, reads, keys, kinds) = (
                recorder.clone(),
                violations.clone(),
                slot.clone(),
                reads.clone(),
                keys.clone(),
                kinds.clone(),
            );
            let clock = env.client_clock(c);
            let db = (t == 0).then(|| open_db(&env, c, &rules, consistency, small));
            futs.push(Box::pin(async move {
                use moraine::clock::Clock;
                // Task 0 opens the handle; the others share it (group commit, case C16).
                let table = match db {
                    Some(db) => {
                        let mut table = None;
                        if let Ok(db) = db.await {
                            for _ in 0..10 {
                                if let Ok(t) = db.open_table(TABLE).await {
                                    table = Some(t);
                                    break;
                                }
                            }
                        }
                        *slot.lock().unwrap() = Some(table.clone());
                        table
                    }
                    None => loop {
                        let got = slot.lock().unwrap().clone();
                        if let Some(t) = got {
                            break t;
                        }
                        clock.sleep(Duration::from_millis(50)).await;
                    },
                };
                let Some(table) = table else {
                    return;
                };
                for j in 0..ops_per_task {
                    let op = format!("c{c}t{t}o{j}");
                    let kind = pick(&mut rng, mode, c, j);
                    kinds.lock().unwrap().insert(op.clone(), kind);
                    let extras = extra_columns(&table);
                    let random_keys = |rng: &mut SimRng| -> Vec<String> {
                        let n = rng.range(1, keys.len() as u64 + 1);
                        let mut ks: BTreeSet<String> = BTreeSet::new();
                        for _ in 0..n {
                            ks.insert(keys[rng.below(keys.len() as u64) as usize].clone());
                        }
                        ks.into_iter().collect()
                    };
                    let new_rows = |rng: &mut SimRng, ks: &[String]| -> Vec<SimRow> {
                        ks.iter()
                            .enumerate()
                            .map(|(n, k)| SimRow {
                                k: k.clone(),
                                op: op.clone(),
                                i: n as i64,
                                g: (!rng.chance(0.2)).then(|| rng.below(3) as i64),
                                extra: match extras.last() {
                                    Some(x) if rng.chance(0.7) => {
                                        BTreeMap::from([(x.clone(), rng.below(100) as i64)])
                                    }
                                    _ => BTreeMap::new(),
                                },
                                keyed: true,
                            })
                            .collect()
                    };
                    // `expected` is an error the operation must return (and then the write is
                    // not visible); None means it must succeed when there are no faults.
                    let (r, expected): (moraine::Result<moraine::Committed>, Option<&str>) = match kind {
                        Kind::Upsert => {
                            let ks = if mode == Mode::SameKey {
                                vec![format!("new#{j}")]
                            } else {
                                random_keys(&mut rng)
                            };
                            let mut rows = new_rows(&mut rng, &ks);
                            // Case D2: a key twice in one batch.
                            let dup = mode != Mode::SameKey && rng.chance(0.2);
                            let last_wins = dup && rng.chance(0.6);
                            let mut sent = rows.clone();
                            if dup {
                                let mut again = rows[rng.below(rows.len() as u64) as usize].clone();
                                again.i = 100 + j as i64;
                                sent.push(again.clone());
                                rows.retain(|r| r.k != again.k);
                                rows.push(again);
                            }
                            rows.sort();
                            rec.intend(task, &op, ModelOp::Upsert(rows));
                            let b = to_batch(&sent, &extras);
                            if dup && !last_wins {
                                (table.upsert(vec![b]).await, Some("DuplicateKeyInBatch"))
                            } else if last_wins {
                                (
                                    table.upsert_with(vec![b], DuplicatePolicy::LastWins).await,
                                    None,
                                )
                            } else {
                                (table.upsert(vec![b]).await, None)
                            }
                        }
                        Kind::Append => {
                            let ks = random_keys(&mut rng);
                            let mut rows = new_rows(&mut rng, &ks);
                            rows.sort();
                            rec.intend(task, &op, ModelOp::Append(rows.clone()));
                            let r = table.append(vec![to_batch(&rows, &extras)]).await;
                            (r, enforce.then_some("PrimaryKeyEnforced"))
                        }
                        Kind::DeleteKeys => {
                            let ks = random_keys(&mut rng);
                            rec.intend(task, &op, ModelOp::DeleteKeys(ks.clone()));
                            let key_values: Vec<Vec<ScalarValue>> = ks
                                .iter()
                                .map(|k| vec![ScalarValue::Utf8(Some(k.clone()))])
                                .collect();
                            (table.delete_keys(&key_values).await, None)
                        }
                        Kind::DeleteWhere => {
                            let p = if rng.chance(0.5) {
                                Pred::Keys(random_keys(&mut rng))
                            } else {
                                Pred::Group((!rng.chance(0.2)).then(|| rng.below(3) as i64))
                            };
                            rec.intend(task, &op, ModelOp::DeleteWhere(p.clone()));
                            (table.delete(p.expr()).await, None)
                        }
                        Kind::Update => {
                            let p = if rng.chance(0.5) {
                                Pred::Keys(random_keys(&mut rng))
                            } else {
                                Pred::Group((!rng.chance(0.2)).then(|| rng.below(3) as i64))
                            };
                            let patch = Patch {
                                op: op.clone(),
                                i: 1000 + j as i64,
                                g: rng
                                    .chance(0.4)
                                    .then(|| (!rng.chance(0.2)).then(|| rng.below(3) as i64)),
                            };
                            rec.intend(task, &op, ModelOp::Update(p.clone(), patch.clone()));
                            (table.update(p.expr(), patch.assignments()).await, None)
                        }
                        Kind::AddColumn => {
                            let name = format!("x{op}");
                            rec.intend(task, &op, ModelOp::Schema(name.clone()));
                            let f = Field::new(&name, DataType::Int64, true);
                            (table.add_column(f).await, None)
                        }
                    };
                    let outcome = match &r {
                        Ok(c) => Outcome::Acked { version: c.version },
                        Err(Error::OutcomeUnknown { .. }) => Outcome::Unknown,
                        Err(e) => Outcome::Failed(e.to_string()),
                    };
                    match (expected, &r) {
                        (Some("DuplicateKeyInBatch"), Err(Error::DuplicateKeyInBatch(_))) => {}
                        (Some("PrimaryKeyEnforced"), Err(Error::PrimaryKeyEnforced)) => {}
                        (Some(want), got) => viol
                            .lock()
                            .unwrap()
                            .push(format!("{kind:?} {op}: expected {want}, got {got:?}")),
                        (None, Err(e)) if mode != Mode::Random => viol
                            .lock()
                            .unwrap()
                            .push(format!("{mode:?}: {kind:?} {op} failed without faults: {e:?}")),
                        (None, _) => {}
                    }
                    rec.outcome(task, &op, outcome);

                    // A read after every write: I3 at its version, I5 for this task.
                    let Ok(snap) = table.snapshot().await else {
                        continue;
                    };
                    let Ok(b) = snap.scan().collect().await else {
                        continue;
                    };
                    let rows = to_rows(&b, true);
                    rec.read(task, snap.version(), rows.clone());
                    *reads.lock().unwrap() += 1;
                    // A point lookup agrees with the scan of the same snapshot.
                    if enforce {
                        let k = keys[rng.below(keys.len() as u64) as usize].clone();
                        let want: Vec<SimRow> = rows.iter().filter(|r| r.k == k).cloned().collect();
                        if let Ok(got) = snap.get(&[ScalarValue::Utf8(Some(k.clone()))]).await {
                            let got = got.map(|b| to_rows(&[b], true)).unwrap_or_default();
                            if got != want {
                                viol.lock().unwrap().push(format!(
                                    "get({k:?}) at version {} returned {got:?}, the scan has {want:?}",
                                    snap.version()
                                ));
                            }
                        }
                    }
                }
            }));
        }
    }
    env.run(futs)
        .await
        .map_err(|e| format!("run did not finish: {e:?}"))?;
    if let Some(v) = violations.lock().unwrap().first() {
        return Err(v.clone());
    }

    // A fresh client without faults reads the final version through moraine.
    let fresh = open_db(&env, 99, &FaultRules::default(), Consistency::Strong, false);
    let rec = recorder.clone();
    env.run(vec![Box::pin(async move {
        let db = fresh.await.expect("open");
        let t = db.open_table(TABLE).await.expect("open table");
        let s = t.snapshot().await.expect("snapshot");
        let b = s.scan().collect().await.expect("scan");
        rec.read(999, s.version(), to_rows(&b, true));
    }) as ClientFuture])
        .await
        .map_err(|e| format!("final read did not finish: {e:?}"))?;

    let kinds = kinds.lock().unwrap().clone();
    let (log, physical, stats) = committed_history(&env, &recorder, &kinds)?;
    let history = recorder.history(log).with_physical(physical);

    let (acked, failed, unknown) = recorder.outcome_counts();
    {
        let mut t = totals.lock().unwrap();
        t.seeds += 1;
        t.ops += kinds.len() as u64;
        t.acked += acked as u64;
        t.failed += failed as u64;
        t.unknown += unknown as u64;
        t.versions += stats.versions;
        t.grouped_versions += stats.grouped;
        t.rewritten_files += stats.removed_files;
        for k in kinds.values() {
            match k {
                Kind::Upsert => t.upserts += 1,
                Kind::DeleteKeys | Kind::DeleteWhere => t.deletes += 1,
                Kind::Update => t.updates += 1,
                Kind::AddColumn => t.add_columns += 1,
                Kind::Append => t.appends += 1,
            }
        }
        t.reads += *reads.lock().unwrap();
        t.requests += env.requests.snapshot().requests();
        t.faults += env.faults.total();
        t.ambiguous_faults += env.faults.ambiguous();
    }
    history.check(enforce).map_err(|v| v.to_string())
}

struct LogStats {
    versions: u64,
    grouped: u64,
    removed_files: u64,
}

type LogLine = (u64, String, ModelOp<SimRow>);
/// The log for the recorder, the rows stored at every version, and counts.
type Committed = (Vec<LogLine>, BTreeMap<u64, Vec<SimRow>>, LogStats);

/// Reads the committed history from the store and explains every version by the operations
/// that wrote it. Returns the log for [`Recorder::history`], the rows stored at every version,
/// and counts.
fn committed_history(
    env: &SimEnv,
    rec: &Recorder<SimRow>,
    kinds: &BTreeMap<String, Kind>,
) -> Result<Committed, String> {
    let root = format!("{ROOT}/{TABLE}/data");
    let entries = read_log(&env.store, &root);
    let intended = rec.intended();
    let outcomes = rec.outcomes();
    let mut files: BTreeMap<String, Vec<SimRow>> = BTreeMap::new();
    let mut physical: BTreeMap<u64, Vec<SimRow>> = BTreeMap::new();
    let mut log: Vec<LogLine> = Vec::new();
    let mut model: TableModel<SimRow> = TableModel::default();
    let mut placed: BTreeSet<String> = BTreeSet::new();
    let mut stats = LogStats {
        versions: entries.len().saturating_sub(1) as u64,
        grouped: 0,
        removed_files: 0,
    };
    let mut columns: BTreeSet<String> = BTreeSet::new();
    for e in entries.iter() {
        let mut added: Vec<SimRow> = Vec::new();
        let mut removed: Vec<SimRow> = Vec::new();
        for p in &e.removes {
            let Some(rows) = files.remove(p) else {
                return Err(format!(
                    "version {} removes {p}, which is not live",
                    e.version
                ));
            };
            removed.extend(rows);
        }
        stats.removed_files += e.removes.len() as u64;
        for p in &e.adds {
            let rows = to_rows(&read_parquet(&env.store, &format!("{root}/{p}")), true);
            added.extend(rows.clone());
            if files.insert(p.clone(), rows).is_some() {
                return Err(format!("version {} adds {p} twice", e.version));
            }
        }
        let stored: Vec<SimRow> = files.values().flatten().cloned().collect();
        if e.version == 0 {
            columns.extend(e.columns.iter().cloned());
            // Writes that found nothing to change in the empty table.
            for (op, o) in &outcomes {
                if *o == (Outcome::Acked { version: 0 })
                    && let Some(w) = intended.get(op)
                {
                    model
                        .apply_more(0, w)
                        .map_err(|err| format!("I3 violated: {err}"))?;
                    placed.insert(op.clone());
                    log.push((0, op.clone(), w.clone()));
                }
            }
            physical.insert(0, stored);
            continue;
        }
        // Rows this version wrote: added minus carried over from the files it rewrote.
        let mut carried: BTreeMap<SimRow, usize> = BTreeMap::new();
        for r in removed {
            *carried.entry(r).or_default() += 1;
        }
        let mut by_op: BTreeMap<String, Vec<SimRow>> = BTreeMap::new();
        for r in added {
            match carried.get_mut(&r) {
                Some(n) if *n > 0 => *n -= 1,
                _ => by_op.entry(r.op.clone()).or_default().push(r),
            }
        }
        // Operations with a trace: rows they wrote, columns they added.
        let mut traced: Vec<(String, ModelOp<SimRow>)> = Vec::new();
        for (op, mut rows) in by_op {
            rows.sort();
            let what = match kinds.get(&op) {
                Some(Kind::Upsert) => ModelOp::Upsert(rows),
                Some(Kind::Append) => ModelOp::Append(rows),
                Some(Kind::Update) => intended
                    .get(&op)
                    .cloned()
                    .ok_or_else(|| format!("update {op} not intended"))?,
                _ => {
                    return Err(format!(
                        "version {} holds rows of {op}, which writes no rows",
                        e.version
                    ));
                }
            };
            traced.push((op, what));
        }
        for c in &e.columns {
            if columns.insert(c.clone()) {
                let op = c.trim_start_matches('x').to_string();
                traced.push((op.clone(), ModelOp::Schema(c.clone())));
            }
        }
        if traced.len() > 1 {
            stats.grouped += 1;
        }
        // Operations without one, acknowledged with this version: deletes, and writes that
        // matched nothing.
        let untraced: Vec<(String, ModelOp<SimRow>)> = outcomes
            .iter()
            .filter(|(op, o)| {
                matches!(o, Outcome::Acked { version } if *version == e.version)
                    && !traced.iter().any(|(t, _)| t == *op)
            })
            .filter_map(|(op, _)| intended.get(op).map(|w| (op.clone(), w.clone())))
            .collect();
        // Deletes whose outcome is unknown may be in any version.
        let unknown: Vec<(String, ModelOp<SimRow>)> = outcomes
            .iter()
            .filter(|(op, o)| **o == Outcome::Unknown && !placed.contains(*op))
            .filter(|(op, _)| matches!(kinds.get(*op), Some(Kind::DeleteKeys | Kind::DeleteWhere)))
            .filter_map(|(op, _)| intended.get(op).map(|w| (op.clone(), w.clone())))
            .collect();
        let order = explain(&model, e.version, &traced, &untraced, &unknown, &stored)
            .unwrap_or_else(|| traced.iter().chain(&untraced).cloned().collect());
        for (i, (op, what)) in order.iter().enumerate() {
            let r = if i == 0 {
                model.apply(e.version, what)
            } else {
                model.apply_more(e.version, what)
            };
            if let Err(err) = r {
                return Err(format!("I3 violated: {err}"));
            }
            placed.insert(op.clone());
            log.push((e.version, op.clone(), what.clone()));
        }
        if order.is_empty() {
            // Nothing explains this version; the checker reports the gap.
            continue;
        }
        physical.insert(e.version, stored);
    }
    Ok((log, physical, stats))
}

/// An order of this version's operations under which the model stores exactly `stored`, or
/// `None`.
///
/// Only deletes leave no trace and can still change rows; every other untraced operation (an
/// update that matched nothing) must leave the stored rows as they are. So the search is over
/// which deletes took effect: traced operations first, then the deletes that took effect (in any
/// order, as deletes commute), then everything else, which must change nothing. Deletes whose
/// outcome is unknown may be among the effective ones or not committed at all.
fn explain(
    model: &TableModel<SimRow>,
    version: u64,
    traced: &[(String, ModelOp<SimRow>)],
    untraced: &[(String, ModelOp<SimRow>)],
    unknown: &[(String, ModelOp<SimRow>)],
    stored: &[SimRow],
) -> Option<Vec<(String, ModelOp<SimRow>)>> {
    let mut want = stored.to_vec();
    want.sort();
    let is_delete =
        |w: &ModelOp<SimRow>| matches!(w, ModelOp::DeleteKeys(_) | ModelOp::DeleteWhere(_));
    let deletes: Vec<(&(String, ModelOp<SimRow>), bool)> = untraced
        .iter()
        .filter(|(_, w)| is_delete(w))
        .map(|o| (o, false))
        .chain(unknown.iter().map(|o| (o, true)))
        .collect();
    let others: Vec<&(String, ModelOp<SimRow>)> =
        untraced.iter().filter(|(_, w)| !is_delete(w)).collect();
    if deletes.len() > 16 {
        return None;
    }
    for mask in 0u32..(1 << deletes.len()) {
        let mut ops: Vec<(String, ModelOp<SimRow>)> = traced.to_vec();
        let mut rest: Vec<(String, ModelOp<SimRow>)> = Vec::new();
        for (i, (o, unknown)) in deletes.iter().enumerate() {
            if mask & (1 << i) != 0 {
                ops.push((*o).clone());
            } else if !unknown {
                rest.push((*o).clone());
            }
        }
        ops.extend(rest);
        ops.extend(others.iter().map(|o| (*o).clone()));
        if ops.is_empty() {
            continue;
        }
        let mut m = model.clone();
        let applied = ops.iter().enumerate().all(|(i, (_, w))| {
            if i == 0 {
                m.apply(version, w).is_ok()
            } else {
                m.apply_more(version, w).is_ok()
            }
        });
        if applied && m.rows() == want {
            return Some(ops);
        }
    }
    None
}

fn run(mode: Mode, seeds: std::ops::Range<u64>) -> Totals {
    let totals = Arc::new(Mutex::new(Totals::default()));
    let t = totals.clone();
    if let Err(f) = run_seeds(seeds.clone(), move |env| scenario(env, mode, t.clone())) {
        panic!("{f}");
    }
    let t = std::mem::take(&mut *totals.lock().unwrap());
    println!(
        "RESULT mutations_sim {mode:?} seeds={}..{} {t:?}",
        seeds.start, seeds.end
    );
    t
}

/// Everything at once, with random faults (T3 and T4 for upserts, deletes and updates).
#[test]
fn mutations_random() {
    let t = run(Mode::Random, seeds_from_env(300));
    assert!(t.acked > 0 && t.failed > 0 && t.faults > 0);
    assert!(t.rewritten_files > 0 && t.add_columns > 0);
}

/// The same mix without faults: every write that should succeed does.
#[test]
fn mutations_no_faults() {
    let t = run(Mode::NoFaults, seeds_from_env(300));
    assert!(
        t.grouped_versions > 0,
        "group commit never merged two callers"
    );
    assert_eq!(t.unknown, 0);
}

/// Scenario 2: two writers upsert the same new key, round after round; exactly one row per key
/// at every version (I4), and both writers' upserts commit (case C2).
#[test]
fn upsert_same_key_race() {
    let t = run(Mode::SameKey, seeds_from_env(300));
    assert_eq!(t.acked, t.ops, "every upsert commits");
}

/// Scenario 15: columns are added while appends with the old schema are in flight (case C10).
/// Every append commits; its rows read the new columns as null.
#[test]
fn schema_add_column_race() {
    let t = run(Mode::AddColumnRace, seeds_from_env(300));
    assert_eq!(t.acked, t.ops, "every append and add_column commits");
    assert!(t.add_columns > 0);
}

#[test]
fn regression_seeds() {
    for &(seed, mode) in REGRESSION_SEEDS {
        run(mode, seed..seed + 1);
    }
}
