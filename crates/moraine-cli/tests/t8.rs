//! T8: `moraine stress` processes on one table for a while, with random
//! `kill -9` and restarts, maintenance (leases on) in three of them; then a final ingest and
//! maintenance pass, `verify`, and the invariant checks: no row twice (I4), every acknowledged
//! write present (I1, I2), every queued batch committed once (D51).
//!
//! Local disk by default. Ignored in the per-commit suite; run it with
//!
//!     MORAINE_T8_SECONDS=600 cargo test --release -p moraine-cli --test t8 -- --ignored
//!
//! (`MORAINE_T8_PROCESSES`, default 8; `MORAINE_T8_LOCATION` for another store).

use std::collections::{BTreeMap, HashSet};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use moraine::{Database, MaintainOptions, TableOptions};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (z ^ (z >> 31)) % n.max(1)
    }
}

struct Worker {
    name: String,
    maintain: bool,
    child: Option<Child>,
}

fn spawn(location: &str, dir: &std::path::Path, w: &Worker, seconds: u64) -> Child {
    let mut c = Command::new(env!("CARGO_BIN_EXE_moraine"));
    c.args([location, "stress", "t", "--seconds", &seconds.to_string()])
        .args(["--process", &w.name])
        .env("MORAINE_STRESS_ACKS", dir.join(format!("{}.acks", w.name)))
        .stdout(Stdio::null())
        .stderr(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join(format!("{}.err", w.name)))
                .expect("stderr file"),
        );
    if w.maintain {
        c.arg("--maintain");
    }
    c.spawn().expect("spawn moraine stress")
}

#[test]
#[ignore = "T8 runs for minutes; set MORAINE_T8_SECONDS and pass --ignored"]
fn t8_stress_with_kills() {
    let seconds = env_u64("MORAINE_T8_SECONDS", 30);
    let processes = env_u64("MORAINE_T8_PROCESSES", 8) as usize;
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("db");
    std::fs::create_dir_all(&data).unwrap();
    let location = std::env::var("MORAINE_T8_LOCATION")
        .unwrap_or_else(|_| format!("file://{}", data.display()));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("process", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("n", DataType::Int64, false),
    ]));
    rt.block_on(async {
        let db = Database::builder(&location).open().await.unwrap();
        db.create_table(
            "t",
            schema,
            TableOptions::default().grace_period(Duration::from_secs(600)),
        )
        .await
        .unwrap();
    });

    let mut workers: Vec<Worker> = (0..processes)
        .map(|i| Worker {
            name: format!("p{i}"),
            maintain: i < 3,
            child: None,
        })
        .collect();
    let start = Instant::now();
    let deadline = Duration::from_secs(seconds);
    for w in &mut workers {
        w.child = Some(spawn(&location, tmp.path(), w, seconds));
    }
    let mut rng = Rng(env_u64("MORAINE_T8_SEED", 1));
    let mut kills = 0;
    while start.elapsed() + Duration::from_secs(3) < deadline {
        std::thread::sleep(Duration::from_millis(500 + rng.below(4_000)));
        // A worker that exited on its own before the deadline failed.
        for w in &mut workers {
            if let Some(status) = w.child.as_mut().and_then(|c| c.try_wait().unwrap()) {
                let err = std::fs::read_to_string(tmp.path().join(format!("{}.err", w.name)))
                    .unwrap_or_default();
                assert!(
                    status.success() && start.elapsed() + Duration::from_secs(2) >= deadline,
                    "{} exited early with {status}: {err}",
                    w.name
                );
            }
        }
        let w = &mut workers[rng.below(processes as u64) as usize];
        if let Some(mut c) = w.child.take() {
            let _ = c.kill();
            let _ = c.wait();
            kills += 1;
        }
        let left = deadline.saturating_sub(start.elapsed()).as_secs().max(1);
        w.child = Some(spawn(&location, tmp.path(), w, left));
    }
    for w in &mut workers {
        if let Some(mut c) = w.child.take() {
            let status = c.wait().unwrap();
            let err = std::fs::read_to_string(tmp.path().join(format!("{}.err", w.name)))
                .unwrap_or_default();
            assert!(status.success(), "{} failed with {status}: {err}", w.name);
        }
    }

    rt.block_on(async {
        let db = Database::builder(&location).open().await.unwrap();
        let t = db.open_table("t").await.unwrap();
        for _ in 0..3 {
            t.ingest(false).await.unwrap();
        }
        let opts = MaintainOptions::default().leases(false);
        t.maintain(&opts).await.unwrap();
        let v = t.verify().await.unwrap();
        assert!(v.is_ok(), "{v:?}");
        let batches: Vec<RecordBatch> = t.scan().collect().await.unwrap();
        let mut ids: HashSet<String> = HashSet::new();
        let mut rows = 0usize;
        for b in &batches {
            let id = b.column_by_name("id").unwrap().as_string::<i32>();
            for r in 0..b.num_rows() {
                rows += 1;
                assert!(
                    ids.insert(id.value(r).to_string()),
                    "row {} twice",
                    id.value(r)
                );
            }
        }
        let mut acked: BTreeMap<&str, usize> = BTreeMap::new();
        for w in &workers {
            let acks = std::fs::read_to_string(tmp.path().join(format!("{}.acks", w.name)))
                .unwrap_or_default();
            for line in acks.lines() {
                let f: Vec<&str> = line.split(' ').collect();
                match f.as_slice() {
                    ["append", app, seq, kind, n] => {
                        *acked.entry("append").or_default() += 1;
                        if *kind != "keep" {
                            continue;
                        }
                        let run = app.rsplit('.').next().unwrap();
                        for i in 0..n.parse::<usize>().unwrap() {
                            let id = format!("{}.{run}.{seq}.{i}", w.name);
                            assert!(ids.contains(&id), "acknowledged row {id} is missing");
                        }
                    }
                    ["queued", run, qseq, n, _, _] => {
                        *acked.entry("queued").or_default() += 1;
                        for i in 0..n.parse::<usize>().unwrap() {
                            let id = format!("{}.{run}.{qseq}.{i}", w.name);
                            assert!(ids.contains(&id), "queued row {id} is missing");
                        }
                    }
                    ["delete", _] => *acked.entry("delete").or_default() += 1,
                    other => panic!("bad ack line {other:?}"),
                }
            }
        }
        let snap = t.snapshot().await.unwrap();
        println!(
            "RESULT t8 seconds={seconds} processes={processes} kills={kills} rows={rows} \
             files={} version={} acked={acked:?}",
            snap.num_files(),
            snap.version()
        );
    });
}
