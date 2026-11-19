//! moraine-bench: the same workloads on Lance and Delta Lake (through kv0) and on moraine.
//!
//! ```text
//! moraine-bench run    [--engines lance,delta,moraine,moraine-own,moraine-eventual]
//!                      [--backend local|s3] [--scale full|quick|smoke] [--repeat N]
//!                      [--workloads log,records_key,records_cluster,multi_writer]
//!                      [--mw-writers 2,4,8,16] [--kv0-maintenance on]
//!                      [--workdir DIR] [--out results.jsonl]
//! moraine-bench report results.jsonl [more.jsonl ...]    # Markdown tables on stdout
//! moraine-bench vector-d24 [--rows ...] [--dims ...] [--layouts ...]   # D24 ([`vector`])
//! moraine-bench fts [--rows ...] [--layouts ...] [--queries ...]       # D46 ([`fts`])
//! ```
//!
//! `run` starts one child process per workload phase (`moraine-bench child ...`), so every phase
//! starts process-cold (the operating system's page cache stays warm) with its own peak-memory
//! figure, and N writer processes for the multi-writer workload. Each child prints one JSON
//! line; `run` appends them to `--out` with the repetition number. `--repeat` runs everything
//! again, engines interleaved, and the report shows the median with the range.
//!
//! The comparison is kept like for like (docs/BENCH.md, "Fair comparison"): kv0's background
//! maintenance is off in every timed phase (`--kv0-maintenance on` restores it) and compaction
//! is a phase of its own; moraine reads with strong consistency in kv0's metric layout, with its
//! own layout and eventual consistency as extra columns; every read returns values that are
//! checked against what was written.
//!
//! With `--backend s3`, the S3-compatible server from `spike/s3-server.sh` must be running
//! (`MORAINE_SPIKE_S3_ENDPOINT`, `MORAINE_SPIKE_S3_BUCKET` and the AWS variables it prints), and
//! each child counts its requests through a local proxy ([`proxy`]).

mod blobs_and_routes;
mod engine;
mod fts;
mod moraine_engine;
mod proxy;
mod vector;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use engine::{BenchError, Engine, Kind, RunLog, SeriesLog};
use serde_json::{Map, Value, json};

/// Workload sizes. `full` mirrors kv0's benchmark gate.
#[derive(Clone, Copy, Debug)]
struct Scale {
    runs: usize,
    /// Points of `loss[train]` per run; `loss[val]` and `accuracy[val]` add one each every 10th
    /// step, so a run has `1.2 x points` points.
    points: usize,
    /// Rows per records table in the key-lookup workload.
    records: usize,
    /// Runs x items in the cluster-key workload.
    cluster_runs: usize,
    cluster_items: usize,
    writers: usize,
    appends: usize,
    rows_per_append: usize,
}

impl Scale {
    fn named(name: &str) -> Scale {
        match name {
            "full" => Scale {
                runs: 2_000,
                points: 2_000,
                records: 1_000_000,
                cluster_runs: 2_000,
                cluster_items: 500,
                writers: 8,
                appends: 50,
                rows_per_append: 100,
            },
            "quick" => Scale {
                runs: 200,
                points: 2_000,
                records: 100_000,
                cluster_runs: 200,
                cluster_items: 500,
                writers: 8,
                appends: 20,
                rows_per_append: 100,
            },
            // A seconds-long run that checks the harness itself.
            "smoke" => Scale {
                runs: 5,
                points: 100,
                records: 2_000,
                cluster_runs: 66,
                cluster_items: 20,
                writers: 2,
                appends: 3,
                rows_per_append: 10,
            },
            other => panic!("unknown scale {other}"),
        }
    }

    fn to_args(self) -> Vec<String> {
        [
            ("--runs", self.runs),
            ("--points", self.points),
            ("--records", self.records),
            ("--cluster-runs", self.cluster_runs),
            ("--cluster-items", self.cluster_items),
            ("--writers", self.writers),
            ("--appends", self.appends),
            ("--rows-per-append", self.rows_per_append),
        ]
        .iter()
        .flat_map(|(k, v)| [k.to_string(), v.to_string()])
        .collect()
    }
}

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn arg_usize(args: &[String], name: &str, default: usize) -> usize {
    arg(args, name)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn scale_from(args: &[String]) -> Scale {
    let d = Scale::named("quick");
    Scale {
        runs: arg_usize(args, "--runs", d.runs),
        points: arg_usize(args, "--points", d.points),
        records: arg_usize(args, "--records", d.records),
        cluster_runs: arg_usize(args, "--cluster-runs", d.cluster_runs),
        cluster_items: arg_usize(args, "--cluster-items", d.cluster_items),
        writers: arg_usize(args, "--writers", d.writers),
        appends: arg_usize(args, "--appends", d.appends),
        rows_per_append: arg_usize(args, "--rows-per-append", d.rows_per_append),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    match args.first().map(String::as_str) {
        Some("run") => rt.block_on(driver(&args[1..])),
        Some("child") => {
            let line = rt.block_on(child(&args[1..]));
            println!("{line}");
        }
        Some("report") => report(&args[1..]),
        Some("vector-d24") => rt.block_on(vector::run(&args[1..])),
        Some("fts") => rt.block_on(fts::run(&args[1..])),
        Some("blobs") => rt.block_on(blobs_and_routes::blobs(&args[1..])),
        Some("exact-route") => rt.block_on(blobs_and_routes::exact_route(&args[1..])),
        Some("vector-child") => {
            let line = rt.block_on(vector::child(&args[1..]));
            println!("{line}");
        }
        _ => {
            eprintln!("usage: moraine-bench run|report ... (see the source header)");
            std::process::exit(2);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Measurements

fn peak_rss_mb() -> f64 {
    proc_field("/proc/self/status", "VmHWM:").map_or(0.0, |kb| kb / 1024.0)
}

/// Bytes the process read through system calls (`rchar`): the local-disk counterpart of the
/// bytes an object store sends.
fn read_bytes() -> f64 {
    proc_field("/proc/self/io", "rchar:").unwrap_or(0.0)
}

fn proc_field(file: &str, key: &str) -> Option<f64> {
    let s = std::fs::read_to_string(file).ok()?;
    let line = s.lines().find(|l| l.starts_with(key))?;
    line[key.len()..].split_whitespace().next()?.parse().ok()
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.total_cmp(b));
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i]
}

fn round(x: f64, digits: i32) -> f64 {
    let f = 10f64.powi(digits);
    (x * f).round() / f
}

/// A tiny deterministic generator for workload choices (SplitMix64).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        moraine::testing::splitmix64(self.0)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// ---------------------------------------------------------------------------------------------
// Driver

struct Backend {
    s3: Option<S3Env>,
}

#[derive(Clone)]
struct S3Env {
    endpoint: String,
    bucket: String,
}

impl Backend {
    fn from_args(args: &[String]) -> Backend {
        match arg(args, "--backend").as_deref().unwrap_or("local") {
            "local" => Backend { s3: None },
            "s3" => {
                let endpoint = std::env::var("MORAINE_SPIKE_S3_ENDPOINT")
                    .expect("MORAINE_SPIKE_S3_ENDPOINT (source <(spike/s3-server.sh env))");
                let bucket = std::env::var("MORAINE_SPIKE_S3_BUCKET")
                    .unwrap_or_else(|_| "moraine-spike".into());
                Backend {
                    s3: Some(S3Env { endpoint, bucket }),
                }
            }
            other => panic!("unknown backend {other}"),
        }
    }

    fn name(&self) -> &'static str {
        if self.s3.is_some() { "s3" } else { "local" }
    }

    fn location(&self, workdir: &Path, engine: Kind, what: &str, stamp: u128) -> String {
        match &self.s3 {
            None => workdir
                .join(format!("{}-{what}", engine.name()))
                .to_string_lossy()
                .into_owned(),
            Some(s) => format!("s3://{}/bench-{stamp:x}/{}-{what}", s.bucket, engine.name()),
        }
    }
}

async fn driver(args: &[String]) {
    let engines: Vec<Kind> = arg(args, "--engines")
        .unwrap_or_else(|| "lance,delta,moraine,moraine-own,moraine-eventual".into())
        .split(',')
        .map(|e| Kind::parse(e).unwrap_or_else(|| panic!("unknown engine {e}")))
        .collect();
    let backend = Backend::from_args(args);
    let workloads: Vec<String> = arg(args, "--workloads")
        .unwrap_or_else(|| "log,records_key,records_cluster,multi_writer".into())
        .split(',')
        .map(str::to_string)
        .collect();
    let wants = |w: &str| workloads.iter().any(|x| x == w);
    let kv0_maintenance = arg(args, "--kv0-maintenance").as_deref() == Some("on");
    let repeat = arg_usize(args, "--repeat", 1);
    let scale_name = arg(args, "--scale").unwrap_or_else(|| "quick".into());
    let scale = Scale::named(&scale_name);
    let mw_writers: Vec<usize> = arg(args, "--mw-writers")
        .map(|v| v.split(',').filter_map(|n| n.parse().ok()).collect())
        .unwrap_or_else(|| vec![scale.writers]);
    let workdir = PathBuf::from(arg(args, "--workdir").unwrap_or_else(|| "bench-work".into()));
    let out = PathBuf::from(arg(args, "--out").unwrap_or_else(|| "bench-results.jsonl".into()));
    std::fs::create_dir_all(&workdir).expect("workdir");
    let exe = std::env::current_exe().expect("exe");
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    for rep in 0..repeat {
        for &engine in &engines {
            let ctx = Ctx {
                exe: exe.clone(),
                engine,
                backend: &backend,
                scale,
                scale_name: scale_name.clone(),
                workdir: workdir.clone(),
                out: out.clone(),
                kv0_maintenance,
                rep,
            };
            let loc =
                |what: &str| backend.location(&workdir, engine, &format!("{what}-r{rep}"), stamp);
            let tracker = loc("tracker");
            let keyed = loc("records-key");
            let cluster = loc("records-cluster");
            let mut used = vec![tracker.clone(), keyed.clone(), cluster.clone()];
            // moraine's layout only changes the tracker tables, and its consistency only reads:
            // the variants skip what would measure the headline moraine again.
            let own = engine.name() == "moraine-own";
            let queued = engine.name() == "moraine-queued";
            let variant = !engine.is_kv0() && engine.name() != "moraine" && !queued;

            if wants("log") && !queued && ctx.phase("log", &tracker, &[]).await {
                ctx.size("size_before_optimize", &tracker).await;
                ctx.phase("startup", &tracker, &[]).await;
                ctx.phase("series", &tracker, &[]).await;
                // Compaction is timed on its own; without it (a moraine build without compaction) the
                // "after" phases would measure the same table again, so they are skipped.
                if ctx.phase("optimize", &tracker, &[]).await {
                    ctx.size("size_after_optimize", &tracker).await;
                    ctx.phase("startup", &tracker, &["--label", "after_optimize"])
                        .await;
                    ctx.phase("series", &tracker, &["--label", "after_optimize"])
                        .await;
                }
            }
            for (w, location) in [("records_key", &keyed), ("records_cluster", &cluster)] {
                if !wants(w) || own || queued {
                    continue;
                }
                if ctx.phase(&format!("{w}_write"), location, &[]).await {
                    ctx.phase(&format!("{w}_read"), location, &[]).await;
                    if ctx.phase("optimize", location, &["--label", w]).await {
                        ctx.phase(
                            &format!("{w}_read"),
                            location,
                            &["--label", "after_optimize"],
                        )
                        .await;
                    }
                }
            }
            if wants("multi_writer") && !variant {
                for &n in &mw_writers {
                    let multi = loc(&format!("multi-writer-w{n}"));
                    ctx.with_writers(n).multi_writer(&multi).await;
                    used.push(multi);
                }
            }
            let used: Vec<&str> = used.iter().map(String::as_str).collect();
            ctx.cleanup(&used).await;
        }
    }
}

#[derive(Clone)]
struct Ctx<'a> {
    exe: PathBuf,
    engine: Kind,
    backend: &'a Backend,
    scale: Scale,
    scale_name: String,
    workdir: PathBuf,
    out: PathBuf,
    kv0_maintenance: bool,
    rep: usize,
}

impl Ctx<'_> {
    fn with_writers(&self, n: usize) -> Self {
        let mut c = self.clone();
        c.scale.writers = n;
        c
    }

    fn command(&self, workload: &str, location: &str, extra: &[&str]) -> tokio::process::Command {
        let mut c = tokio::process::Command::new(&self.exe);
        c.arg("child")
            .args(["--engine", self.engine.name(), "--workload", workload])
            .args(["--location", location, "--backend", self.backend.name()])
            .args(["--scale-name", &self.scale_name])
            .args(self.scale.to_args())
            .args(extra)
            .args(if self.kv0_maintenance {
                &[][..]
            } else {
                &["--kv0-maintenance", "off"][..]
            })
            .env(
                "KV0_ENGINE",
                if self.engine == Kind::Delta {
                    "delta"
                } else {
                    "lance"
                },
            )
            .env(
                "KV0_WAL_DIR",
                self.workdir.join(format!("wal-{}", self.engine.name())),
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        c
    }

    /// Appends one result line, tagged with the repetition and kv0's maintenance setting.
    fn record(&self, line: &str) {
        use std::io::Write;
        let mut v: Value = serde_json::from_str(line).unwrap_or_else(|_| json!({ "raw": line }));
        v["rep"] = json!(self.rep);
        v["kv0_maintenance"] = json!(self.kv0_maintenance);
        let line = v.to_string();
        eprintln!("{line}");
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.out)
            .expect("results file");
        writeln!(f, "{line}").expect("write results");
    }

    /// Runs one child phase; true when it succeeded.
    async fn phase(&self, workload: &str, location: &str, extra: &[&str]) -> bool {
        let out = self
            .command(workload, location, extra)
            .output()
            .await
            .expect("spawn child");
        let line = String::from_utf8_lossy(&out.stdout)
            .lines()
            .last()
            .unwrap_or("")
            .to_string();
        let ok = out.status.success() && line.contains("\"status\":\"ok\"");
        if line.starts_with('{') {
            self.record(&line);
        } else {
            self.record(
                &json!({
                    "engine": self.engine.name(), "backend": self.backend.name(),
                    "scale": self.scale_name, "workload": workload, "status": "error",
                    "error": format!("child exited with {}", out.status),
                })
                .to_string(),
            );
        }
        ok
    }

    async fn size(&self, label: &str, location: &str) {
        let bytes = match &self.backend.s3 {
            None => dir_size(Path::new(location)),
            Some(s) => s3_size(s, location).await,
        };
        self.record(
            &json!({
                "engine": self.engine.name(), "backend": self.backend.name(),
                "scale": self.scale_name, "workload": "disk", "label": label, "status": "ok",
                "metrics": { "size_mb": round(bytes as f64 / 1e6, 1) },
            })
            .to_string(),
        );
    }

    async fn multi_writer(&self, location: &str) {
        if !self.phase("mw_init", location, &[]).await {
            return;
        }
        // Every writer opens its handle first and starts appending at the same instant, so the
        // writers overlap the same way whatever each engine's start-up costs.
        let start_at = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 5_000)
            .to_string();
        let began = Instant::now();
        let mut children = Vec::new();
        for w in 0..self.scale.writers {
            let w = w.to_string();
            children.push(
                self.command(
                    "mw_writer",
                    location,
                    &["--writer", &w, "--start-at-ms", &start_at],
                )
                .spawn()
                .expect("spawn writer"),
            );
        }
        let mut lat = Vec::new();
        let mut requests = 0.0;
        let mut rss: f64 = 0.0;
        let mut failed = 0;
        for c in children {
            let out = c.wait_with_output().await.expect("writer");
            let line = String::from_utf8_lossy(&out.stdout)
                .lines()
                .last()
                .unwrap_or("")
                .to_string();
            let v: Value = serde_json::from_str(&line).unwrap_or(Value::Null);
            if v["status"] != "ok" {
                failed += 1;
                continue;
            }
            if let Some(l) = v["metrics"]["latencies_ms"].as_array() {
                lat.extend(l.iter().filter_map(Value::as_f64));
            }
            requests += v["metrics"]["requests"].as_f64().unwrap_or(0.0);
            rss = rss.max(v["metrics"]["peak_rss_mb"].as_f64().unwrap_or(0.0));
        }
        // Wall time counts from the common start, not from spawning.
        let wall = began.elapsed().saturating_sub(Duration::from_secs(5));
        let commits = lat.len();
        let mut m = Map::new();
        m.insert("wall_s".into(), json!(round(wall.as_secs_f64(), 2)));
        m.insert("writers".into(), json!(self.scale.writers));
        m.insert("commits".into(), json!(commits));
        m.insert(
            "commits_per_s".into(),
            json!(round(commits as f64 / wall.as_secs_f64(), 1)),
        );
        m.insert("commit_p50_ms".into(), json!(round(pct(&mut lat, 0.5), 1)));
        m.insert("commit_p99_ms".into(), json!(round(pct(&mut lat, 0.99), 1)));
        m.insert("failed_writers".into(), json!(failed));
        m.insert("peak_rss_mb".into(), json!(round(rss, 0)));
        if self.backend.s3.is_some() && commits > 0 {
            m.insert(
                "requests_per_commit".into(),
                json!(round(requests / commits as f64, 1)),
            );
        }
        let line = json!({
            "engine": self.engine.name(), "backend": self.backend.name(),
            "scale": self.scale_name, "workload": "multi_writer",
            "label": format!("w{}", self.scale.writers),
            "status": if failed == 0 && commits > 0 { "ok" } else { "error" },
            "metrics": m,
        });
        self.record(&line.to_string());
        self.phase(
            "mw_verify",
            location,
            &["--label", &format!("w{}", self.scale.writers)],
        )
        .await;
    }

    async fn cleanup(&self, locations: &[&str]) {
        for l in locations {
            match &self.backend.s3 {
                None => {
                    let _ = std::fs::remove_dir_all(l);
                }
                Some(s) => s3_delete(s, l).await,
            }
        }
        let _ = std::fs::remove_dir_all(self.workdir.join(format!("wal-{}", self.engine.name())));
    }
}

fn dir_size(p: &Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(p) else {
        return std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
    };
    rd.flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() {
                dir_size(&p)
            } else {
                e.metadata().map(|m| m.len()).unwrap_or(0)
            }
        })
        .sum()
}

fn s3_store(s: &S3Env) -> Arc<dyn object_store::ObjectStore> {
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_endpoint(&s.endpoint)
        .with_bucket_name(&s.bucket)
        .with_allow_http(true)
        .build()
        .expect("s3 store");
    Arc::new(store)
}

fn s3_prefix(location: &str) -> object_store::path::Path {
    let rest = location.trim_start_matches("s3://");
    let key = rest.split_once('/').map_or("", |(_, k)| k);
    object_store::path::Path::from(key)
}

async fn s3_size(s: &S3Env, location: &str) -> u64 {
    use futures::TryStreamExt;
    s3_store(s)
        .list(Some(&s3_prefix(location)))
        .try_fold(0u64, |acc, m| async move { Ok(acc + m.size) })
        .await
        .unwrap_or(0)
}

async fn s3_delete(s: &S3Env, location: &str) {
    use futures::{StreamExt, TryStreamExt};
    let store = s3_store(s);
    let paths = store
        .list(Some(&s3_prefix(location)))
        .map_ok(|m| m.location)
        .boxed();
    let _ = store.delete_stream(paths).try_collect::<Vec<_>>().await;
}

// ---------------------------------------------------------------------------------------------
// Child

struct ChildEnv {
    kind: Kind,
    location: String,
    storage: HashMap<String, String>,
    proxy: Option<Arc<proxy::Counters>>,
    scale: Scale,
    kv0_maintenance: bool,
}

async fn child(args: &[String]) -> String {
    let kind = Kind::parse(&arg(args, "--engine").expect("--engine")).expect("engine");
    let workload = arg(args, "--workload").expect("--workload");
    let label = arg(args, "--label");
    let backend = arg(args, "--backend").unwrap_or_else(|| "local".into());
    let mut storage = HashMap::new();
    let mut proxy = None;
    if backend == "s3" {
        let upstream = std::env::var("MORAINE_SPIKE_S3_ENDPOINT").expect("endpoint");
        let host = upstream.trim_start_matches("http://").to_string();
        let (addr, counters) = proxy::start(host).await.expect("proxy");
        let env = |k: &str| std::env::var(k).unwrap_or_default();
        storage.insert("aws_endpoint".into(), format!("http://{addr}"));
        storage.insert("aws_access_key_id".into(), env("AWS_ACCESS_KEY_ID"));
        storage.insert("aws_secret_access_key".into(), env("AWS_SECRET_ACCESS_KEY"));
        storage.insert("aws_region".into(), "us-east-1".into());
        storage.insert("allow_http".into(), "true".into());
        proxy = Some(counters);
    }
    let env = ChildEnv {
        kind,
        location: arg(args, "--location").expect("--location"),
        storage,
        proxy,
        scale: scale_from(args),
        kv0_maintenance: arg(args, "--kv0-maintenance").as_deref() != Some("off"),
    };
    let began = Instant::now();
    let r = match workload.as_str() {
        "log" => w_log(&env).await,
        "startup" => w_startup(&env).await,
        "series" => w_series(&env).await,
        "optimize" => w_optimize(&env).await,
        "records_key_write" => w_records_key_write(&env).await,
        "records_key_read" => w_records_key_read(&env).await,
        "records_cluster_write" => w_records_cluster_write(&env).await,
        "records_cluster_read" => w_records_cluster_read(&env).await,
        "mw_init" => w_mw_init(&env).await,
        "mw_writer" => {
            let start = arg(args, "--start-at-ms").and_then(|v| v.parse().ok());
            w_mw_writer(&env, arg_usize(args, "--writer", 0), start).await
        }
        "mw_verify" => w_mw_verify(&env).await,
        other => Err(BenchError::Failed(format!("unknown workload {other}"))),
    };
    let mut line = json!({
        "engine": kind.name(),
        "backend": backend,
        "scale": arg(args, "--scale-name").unwrap_or_default(),
        "workload": workload,
    });
    if let Some(l) = label {
        line["label"] = json!(l);
    }
    match r {
        Ok(mut m) => {
            m.insert(
                "phase_s".into(),
                json!(round(began.elapsed().as_secs_f64(), 2)),
            );
            m.insert("peak_rss_mb".into(), json!(round(peak_rss_mb(), 0)));
            match &env.proxy {
                Some(c) => {
                    let (by_method, up, down) = c.snapshot();
                    m.insert("requests".into(), json!(c.requests()));
                    m.insert("requests_by_method".into(), json!(by_method));
                    m.insert("bytes_read_mb".into(), json!(round(down as f64 / 1e6, 2)));
                    m.insert("bytes_sent_mb".into(), json!(round(up as f64 / 1e6, 2)));
                }
                None => {
                    m.insert("bytes_read_mb".into(), json!(round(read_bytes() / 1e6, 2)));
                }
            }
            line["status"] = json!("ok");
            line["metrics"] = Value::Object(m);
        }
        Err(BenchError::NotImplemented) => line["status"] = json!("not_implemented"),
        Err(BenchError::Failed(e)) => {
            line["status"] = json!("error");
            line["error"] = json!(e);
        }
    }
    line.to_string()
}

type Metrics = engine::Result<Map<String, Value>>;

async fn open(env: &ChildEnv, background_flush: bool) -> engine::Result<Engine> {
    Engine::open_with(
        env.kind,
        &env.location,
        &env.storage,
        background_flush,
        env.kv0_maintenance,
    )
    .await
}

fn requests_now(env: &ChildEnv) -> u64 {
    env.proxy.as_ref().map_or(0, |c| c.requests())
}

fn fail<T>(msg: String) -> engine::Result<T> {
    Err(BenchError::Failed(msg))
}

/// p50 and p99 of `v` under `name_p50_ms` and `name_p99_ms`.
fn percentiles(m: &mut Map<String, Value>, name: &str, v: &[f64], digits: i32) {
    let mut v = v.to_vec();
    m.insert(
        format!("{name}_p50_ms"),
        json!(round(pct(&mut v, 0.5), digits)),
    );
    m.insert(
        format!("{name}_p99_ms"),
        json!(round(pct(&mut v, 0.99), digits)),
    );
}

/// The logging workload's runs, generated deterministically: run `i` is the same in every
/// process, so a reader can regenerate what was written and compare.
struct RunGen {
    rng: Rng,
    next: usize,
    points: usize,
}

impl RunGen {
    fn new(points: usize) -> Self {
        RunGen {
            rng: Rng(1),
            next: 0,
            points,
        }
    }

    fn next_run(&mut self) -> RunLog {
        let i = self.next;
        self.next += 1;
        let rng = &mut self.rng;
        let lr = 10f64.powf(-4.0 + 3.0 * rng.unit());
        let mk = |name, subset| SeriesLog {
            name,
            subset,
            steps: Vec::new(),
            values: Vec::new(),
        };
        let mut train = mk("loss", "train");
        let mut val = mk("loss", "val");
        let mut acc = mk("accuracy", "val");
        for step in 0..self.points as i64 {
            let noise = (rng.unit() - 0.5) * 0.04;
            let loss = 0.2 + 2.0 * (-(step as f64) / (300.0 + 2000.0 * lr)).exp() + noise;
            train.steps.push(step);
            train.values.push(loss);
            if step % 10 == 0 {
                val.steps.push(step);
                val.values.push(loss * 1.1);
                acc.steps.push(step);
                acc.values.push(1.0 - loss / 3.0);
            }
        }
        let batch_size = [32, 64, 128][rng.below(3) as usize];
        RunLog {
            name: format!("run-{i}"),
            experiment: format!("sweep-{}", i % 20),
            params: json!({ "hparams": { "lr": lr, "batch_size": batch_size, "seed": i } }),
            series: vec![train, val, acc],
        }
    }
}

/// Append-heavy logging: `runs` runs, each with parameters and `1.2 x points` points, one flush
/// per run (kv0's `scale.py` log phase). The logging calls and the flush are timed separately.
async fn w_log(env: &ChildEnv) -> Metrics {
    let s = env.scale;
    let repo = open(env, true).await?;
    let mut runs = RunGen::new(s.points);
    let (mut per_run, mut calls, mut flushes) = (Vec::new(), Vec::new(), Vec::new());
    let mut per_run_requests = Vec::with_capacity(s.runs);
    let began = Instant::now();
    for _ in 0..s.runs {
        let run = runs.next_run();
        let prepared = repo.prepare(&run);
        let req0 = requests_now(env);
        let (c, f) = repo.log_run(&run, prepared).await?;
        per_run.push(ms(c + f));
        calls.push(ms(c));
        flushes.push(ms(f));
        per_run_requests.push((requests_now(env) - req0) as f64);
    }
    repo.close().await?;
    let wall = began.elapsed().as_secs_f64();
    let total_points = s.runs * (s.points + 2 * s.points.div_ceil(10));
    let tenth = (per_run.len() / 10).max(1);
    let first: f64 = per_run[..tenth].iter().sum::<f64>() / tenth as f64;
    let last: f64 = per_run[per_run.len() - tenth..].iter().sum::<f64>() / tenth as f64;
    let mut m = Map::new();
    m.insert("runs".into(), json!(s.runs));
    m.insert("points".into(), json!(total_points));
    m.insert("wall_s".into(), json!(round(wall, 2)));
    m.insert(
        "points_per_s".into(),
        json!(round(total_points as f64 / wall, 0)),
    );
    m.insert("run_first_tenth_ms".into(), json!(round(first, 1)));
    m.insert("run_last_tenth_ms".into(), json!(round(last, 1)));
    percentiles(&mut m, "run", &per_run, 2);
    percentiles(&mut m, "calls", &calls, 2);
    percentiles(&mut m, "flush", &flushes, 2);
    if env.proxy.is_some() {
        m.insert(
            "requests_per_run_p50".into(),
            json!(pct(&mut per_run_requests.clone(), 0.5)),
        );
        m.insert(
            "requests_per_run_mean".into(),
            json!(round(
                per_run_requests.iter().sum::<f64>() / s.runs as f64,
                1
            )),
        );
    }
    Ok(m)
}

/// Cold start-up (process-cold; the page cache stays warm): list runs, then each run's
/// parameters and series (`scale.py` start phase).
async fn w_startup(env: &ChildEnv) -> Metrics {
    let began = Instant::now();
    let repo = open(env, false).await?;
    let runs = repo.runs().await?;
    let mut series = 0;
    for (id, _) in &runs {
        series += repo.run_detail(id).await?;
    }
    let wall = began.elapsed().as_secs_f64();
    repo.close().await?;
    if runs.len() != env.scale.runs || series != 3 * env.scale.runs {
        return fail(format!("{} runs and {series} series listed", runs.len()));
    }
    let mut m = Map::new();
    m.insert("wall_s".into(), json!(round(wall, 2)));
    m.insert("runs".into(), json!(runs.len()));
    m.insert("series".into(), json!(series));
    Ok(m)
}

/// FNV-1a over a series' steps and value bits, so lines from different engines can be compared.
fn checksum(h: &mut u64, points: &[(i64, f64)]) {
    for (s, v) in points {
        for b in s.to_le_bytes().into_iter().chain(v.to_bits().to_le_bytes()) {
            *h ^= u64::from(b);
            *h = h.wrapping_mul(0x0100_0000_01b3);
        }
    }
}

/// Full reads of `loss[train]` from 50 runs and 100-step windows from 50 other runs, each
/// checked point for point against the regenerated run. The first read of each kind is
/// reported on its own (it opens the files); the percentiles are over the other 49.
async fn w_series(env: &ChildEnv) -> Metrics {
    let repo = open(env, false).await?;
    let runs = repo.runs().await?;
    let ids: HashMap<String, String> = runs.into_iter().map(|(id, name)| (name, id)).collect();
    let n = env.scale.runs;
    if ids.len() != n {
        return fail(format!("{} runs listed, expected {n}", ids.len()));
    }
    // 100 distinct runs: the first 50 read whole, the other 50 read as a window.
    let mut rng = Rng(7);
    let mut order: Vec<usize> = (0..n).collect();
    for i in (1..n).rev() {
        order.swap(i, rng.below(i as u64 + 1) as usize);
    }
    let reads = 50.min(n / 2).max(1);
    let full_runs = &order[..reads];
    let window_runs: Vec<usize> = (0..reads).map(|i| order[(reads + i) % n]).collect();
    let wanted: HashSet<usize> = full_runs.iter().chain(&window_runs).copied().collect();
    let mut expected: HashMap<usize, Vec<(i64, f64)>> = HashMap::new();
    let mut gen_ = RunGen::new(env.scale.points);
    for i in 0..n {
        let run = gen_.next_run();
        if wanted.contains(&i) {
            let s = &run.series[0];
            expected.insert(
                i,
                s.steps
                    .iter()
                    .copied()
                    .zip(s.values.iter().copied())
                    .collect(),
            );
        }
    }
    let lo = (env.scale.points / 2) as i64;
    let (mut full, mut window) = (Vec::new(), Vec::new());
    let mut sum = 0xcbf2_9ce4_8422_2325u64;
    for (k, (&f, &w)) in full_runs.iter().zip(&window_runs).enumerate() {
        let id = &ids[&format!("run-{f}")];
        let t0 = Instant::now();
        let got = repo.read_series(id, "loss", "train", None, None).await?;
        full.push(ms(t0.elapsed()));
        if got != expected[&f] {
            return fail(format!(
                "run-{f}: the full series differs ({} points)",
                got.len()
            ));
        }
        checksum(&mut sum, &got);
        let id = &ids[&format!("run-{w}")];
        let t0 = Instant::now();
        let got = repo
            .read_series(id, "loss", "train", Some(lo), Some(lo + 100))
            .await?;
        window.push(ms(t0.elapsed()));
        let want: Vec<(i64, f64)> = expected[&w]
            .iter()
            .copied()
            .filter(|p| p.0 >= lo && p.0 < lo + 100)
            .collect();
        if got != want || (env.scale.points >= 200 && got.len() != 100) {
            return fail(format!(
                "run-{w}: the window differs ({} points)",
                got.len()
            ));
        }
        checksum(&mut sum, &got);
        let _ = k;
    }
    repo.close().await?;
    let mut m = Map::new();
    m.insert("points_per_series".into(), json!(env.scale.points));
    m.insert("verified_reads".into(), json!(2 * reads));
    m.insert("series_checksum".into(), json!(format!("{sum:016x}")));
    m.insert("full_first_ms".into(), json!(round(full[0], 2)));
    m.insert("window_first_ms".into(), json!(round(window[0], 2)));
    percentiles(&mut m, "full", &full[1.min(full.len() - 1)..], 2);
    percentiles(&mut m, "window", &window[1.min(window.len() - 1)..], 2);
    Ok(m)
}

async fn w_optimize(env: &ChildEnv) -> Metrics {
    let repo = open(env, false).await?;
    let t0 = Instant::now();
    repo.optimize().await?;
    let wall = t0.elapsed().as_secs_f64();
    repo.close().await?;
    let mut m = Map::new();
    m.insert("wall_s".into(), json!(round(wall, 2)));
    Ok(m)
}

/// The value item `k` of the key-lookup table holds: its first half was rewritten to `-k`.
fn key_value(k: usize, n: usize) -> i64 {
    if k < n / 2 { -(k as i64) } else { k as i64 }
}

/// Point lookups by key (`records.py`), the writes: two tables of `records` rows, then the
/// first half of one rewritten (kv0: appended and superseded on read; moraine: upserted).
async fn w_records_key_write(env: &ChildEnv) -> Metrics {
    let n = env.scale.records;
    let pad = "x".repeat(160);
    let repo = open(env, false).await?;
    let t0 = Instant::now();
    for table in ["other", "evals"] {
        for lo in (0..n).step_by(20_000) {
            let rows = (lo..n.min(lo + 20_000))
                .map(|k| (format!("i{k}"), None, json!({ "v": k, "pad": pad })))
                .collect();
            repo.write_records(table, rows, false).await?;
        }
    }
    for lo in (0..n / 2).step_by(20_000) {
        let rows = (lo..(n / 2).min(lo + 20_000))
            .map(|k| {
                (
                    format!("i{k}"),
                    None,
                    json!({ "v": key_value(k, n), "pad": pad }),
                )
            })
            .collect();
        repo.write_records("evals", rows, true).await?;
    }
    let write_s = t0.elapsed().as_secs_f64();
    repo.close().await?;
    let mut m = Map::new();
    m.insert("rows".into(), json!(n));
    m.insert("write_s".into(), json!(round(write_s, 2)));
    Ok(m)
}

fn value_of(data: &Value) -> Option<i64> {
    data.get("v").and_then(Value::as_i64)
}

/// The reads: 50 lookups by item id as a filtered scan (what kv0 does), 50 by primary key
/// (moraine only), and one full read; every value is checked, rewritten keys included.
async fn w_records_key_read(env: &ChildEnv) -> Metrics {
    let n = env.scale.records;
    let repo = open(env, false).await?;
    let req0 = requests_now(env);
    let mut rng = Rng(1);
    let mut scans = Vec::new();
    for _ in 0..50 {
        let k = rng.below(n as u64) as usize;
        let t0 = Instant::now();
        let rows = repo
            .records("evals", Some(&format!("i{k}")), None, None)
            .await?;
        scans.push(ms(t0.elapsed()));
        let got: Vec<Option<i64>> = rows.iter().map(|r| value_of(&r.data)).collect();
        if got != [Some(key_value(k, n))] {
            return fail(format!("item i{k}: {got:?}"));
        }
    }
    let lookup_requests = requests_now(env) - req0;
    let mut gets = Vec::new();
    for _ in 0..50 {
        let k = rng.below(n as u64) as usize;
        let t0 = Instant::now();
        match repo.record_by_key("evals", None, &format!("i{k}")).await {
            Ok(v) => {
                gets.push(ms(t0.elapsed()));
                if v.as_ref().and_then(value_of) != Some(key_value(k, n)) {
                    return fail(format!("key i{k}: {v:?}"));
                }
            }
            Err(BenchError::NotImplemented) => break,
            Err(e) => return Err(e),
        }
    }
    let t0 = Instant::now();
    let rows = repo.records("evals", None, None, None).await?;
    let scan_s = t0.elapsed().as_secs_f64();
    repo.close().await?;
    let want: i64 = (0..n).map(|k| key_value(k, n)).sum();
    let sum: i64 = rows.iter().filter_map(|r| value_of(&r.data)).sum();
    if rows.len() != n || sum != want {
        return fail(format!(
            "full read: {} rows, sum {sum}, expected {n} and {want}",
            rows.len()
        ));
    }
    let mut m = Map::new();
    m.insert("rows".into(), json!(n));
    m.insert("lookup_first_ms".into(), json!(round(scans[0], 2)));
    percentiles(&mut m, "lookup", &scans[1..], 2);
    if !gets.is_empty() {
        percentiles(&mut m, "get", &gets, 2);
    }
    m.insert("scan_s".into(), json!(round(scan_s, 2)));
    m.insert("scan_rows".into(), json!(rows.len()));
    if env.proxy.is_some() {
        m.insert(
            "requests_per_lookup".into(),
            json!(round(lookup_requests as f64 / 50.0, 1)),
        );
    }
    Ok(m)
}

/// Lookups by cluster key (`records_runs.py`), the writes: `cluster_runs x cluster_items` rows.
async fn w_records_cluster_write(env: &ChildEnv) -> Metrics {
    let (runs, items) = (env.scale.cluster_runs, env.scale.cluster_items);
    let repo = open(env, false).await?;
    let t0 = Instant::now();
    for r0 in (0..runs).step_by(40) {
        let rows = (r0..runs.min(r0 + 40))
            .flat_map(|r| {
                (0..items).map(move |i| (format!("q{i}"), Some(format!("r{r}")), json!({ "s": i })))
            })
            .collect();
        repo.write_records("scores", rows, false).await?;
    }
    let write_s = t0.elapsed().as_secs_f64();
    repo.close().await?;
    let mut m = Map::new();
    m.insert("rows".into(), json!(runs * items));
    m.insert("write_s".into(), json!(round(write_s, 2)));
    Ok(m)
}

/// Every row belongs to one of `want`, and each wanted run has all `items` items once.
fn check_runs(rows: &[engine::Rec], want: &[String], items: usize) -> Result<(), String> {
    let mut seen: HashMap<&str, HashSet<i64>> = HashMap::new();
    for r in rows {
        let run = r.run_id.as_deref().unwrap_or("");
        let s = r.data.get("s").and_then(Value::as_i64);
        if !want.iter().any(|w| w == run) || s != r.item_id[1..].parse().ok() {
            return Err(format!("unexpected row {run}/{}: {}", r.item_id, r.data));
        }
        seen.entry(run).or_default().insert(s.unwrap_or(-1));
    }
    if seen.len() != want.len() || seen.values().any(|s| s.len() != items) {
        return Err(format!("{} rows over {} runs", rows.len(), seen.len()));
    }
    Ok(())
}

/// The reads: 31 runs one at a time and in one `IN` query, five rounds; median rounds, with
/// the first query of the process reported on its own. Every result is checked.
async fn w_records_cluster_read(env: &ChildEnv) -> Metrics {
    let (runs, items) = (env.scale.cluster_runs, env.scale.cluster_items);
    let repo = open(env, false).await?;
    let want: Vec<String> = (0..runs)
        .step_by(65)
        .take(31)
        .map(|r| format!("r{r}"))
        .collect();
    let (mut one, mut many, mut per_query) = (Vec::new(), Vec::new(), Vec::new());
    let mut first = None;
    for _ in 0..5 {
        let t0 = Instant::now();
        let rows = repo.records("scores", None, None, Some(&want)).await?;
        one.push(ms(t0.elapsed()));
        check_runs(&rows, &want, items).map_err(BenchError::Failed)?;
        let t0 = Instant::now();
        for r in &want {
            let q0 = Instant::now();
            let rows = repo.records("scores", None, Some(r), None).await?;
            let d = ms(q0.elapsed());
            match first {
                None => first = Some(d),
                Some(_) => per_query.push(d),
            }
            check_runs(&rows, std::slice::from_ref(r), items).map_err(BenchError::Failed)?;
        }
        many.push(ms(t0.elapsed()));
    }
    repo.close().await?;
    let mut m = Map::new();
    m.insert("rows".into(), json!(runs * items));
    m.insert("runs_wanted".into(), json!(want.len()));
    m.insert("in_query_first_ms".into(), json!(round(one[0], 2)));
    m.insert("in_query_ms".into(), json!(round(pct(&mut one, 0.5), 2)));
    m.insert(
        "per_run_queries_ms".into(),
        json!(round(pct(&mut many, 0.5), 2)),
    );
    m.insert(
        "per_run_first_ms".into(),
        json!(round(first.unwrap_or(f64::NAN), 2)),
    );
    percentiles(&mut m, "per_run", &per_query, 2);
    Ok(m)
}

/// Creates the multi-writer table so writers do not race to create it.
async fn w_mw_init(env: &ChildEnv) -> Metrics {
    let repo = open(env, false).await?;
    repo.write_records(
        "events",
        vec![("init".into(), Some("init".into()), json!({}))],
        false,
    )
    .await?;
    repo.close().await?;
    Ok(Map::new())
}

/// One of N writer processes: `appends` commits of `rows_per_append` fresh rows each.
async fn w_mw_writer(env: &ChildEnv, writer: usize, start_at_ms: Option<u128>) -> Metrics {
    let s = env.scale;
    let repo = open(env, false).await?;
    if let Some(at) = start_at_ms {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        if at > now {
            tokio::time::sleep(Duration::from_millis((at - now) as u64)).await;
        }
    }
    let mut lat = Vec::with_capacity(s.appends);
    for a in 0..s.appends {
        let rows = (0..s.rows_per_append)
            .map(|i| {
                (
                    format!("w{writer}-a{a}-i{i}"),
                    Some(format!("w{writer}")),
                    json!({ "a": a, "i": i }),
                )
            })
            .collect();
        let t0 = Instant::now();
        repo.write_records("events", rows, false).await?;
        lat.push(round(ms(t0.elapsed()), 2));
    }
    repo.close().await?;
    let mut m = Map::new();
    m.insert("latencies_ms".into(), json!(lat));
    Ok(m)
}

/// Every row of every writer is present exactly once.
async fn w_mw_verify(env: &ChildEnv) -> Metrics {
    let s = env.scale;
    let repo = open(env, false).await?;
    let n = repo.records("events", None, None, None).await?.len();
    let raw = repo.raw_records("events").await?;
    repo.close().await?;
    let want = s.writers * s.appends * s.rows_per_append + 1;
    if n != want {
        return fail(format!("{n} rows, expected {want}"));
    }
    // kv0's reads keep the newest row per key, so they cannot show a commit applied twice; the
    // raw count can (where the engine offers one).
    if raw.is_some_and(|r| r != want) {
        return fail(format!("{raw:?} rows stored, expected {want}"));
    }
    let mut m = Map::new();
    m.insert("rows".into(), json!(n));
    if let Some(r) = raw {
        m.insert("raw_rows".into(), json!(r));
    }
    Ok(m)
}

// ---------------------------------------------------------------------------------------------
// Report

/// What each table row shows: (workload, label or "", metric, heading).
const ROWS: &[(&str, &str, &str, &str)] = &[
    ("log", "", "wall_s", "Logging: wall time (s)"),
    ("log", "", "points_per_s", "Logging: points per second"),
    (
        "log",
        "",
        "calls_p50_ms",
        "Logging: logging calls of one run, p50 (ms)",
    ),
    (
        "log",
        "",
        "flush_p50_ms",
        "Logging: finish and flush of one run, p50 (ms)",
    ),
    (
        "log",
        "",
        "flush_p99_ms",
        "Logging: finish and flush of one run, p99 (ms)",
    ),
    (
        "log",
        "",
        "run_p50_ms",
        "Logging: one run, calls + finish + flush, p50 (ms)",
    ),
    (
        "log",
        "",
        "run_p99_ms",
        "Logging: one run, calls + finish + flush, p99 (ms)",
    ),
    (
        "log",
        "",
        "run_first_tenth_ms",
        "Logging: mean run, first tenth (ms)",
    ),
    (
        "log",
        "",
        "run_last_tenth_ms",
        "Logging: mean run, last tenth (ms)",
    ),
    (
        "log",
        "",
        "requests_per_run_mean",
        "Logging: requests per run",
    ),
    ("log", "", "peak_rss_mb", "Logging: peak memory (MB)"),
    (
        "disk",
        "size_before_optimize",
        "size_mb",
        "Size on store before compaction (MB)",
    ),
    (
        "startup",
        "",
        "wall_s",
        "Process-cold start-up: runs + params + series per run (s)",
    ),
    ("startup", "", "requests", "Process-cold start-up: requests"),
    (
        "startup",
        "",
        "bytes_read_mb",
        "Process-cold start-up: bytes read (MB)",
    ),
    (
        "series",
        "",
        "full_first_ms",
        "Full series, first read of the process (ms)",
    ),
    ("series", "", "full_p50_ms", "Full series, p50 (ms)"),
    ("series", "", "full_p99_ms", "Full series, p99 (ms)"),
    (
        "series",
        "",
        "window_first_ms",
        "100-step window, first read (ms)",
    ),
    (
        "series",
        "",
        "window_p50_ms",
        "100-step window, other run, p50 (ms)",
    ),
    (
        "series",
        "",
        "window_p99_ms",
        "100-step window, other run, p99 (ms)",
    ),
    (
        "optimize",
        "",
        "wall_s",
        "Compaction of the tracker tables: wall time (s)",
    ),
    (
        "optimize",
        "",
        "peak_rss_mb",
        "Compaction of the tracker tables: peak memory (MB)",
    ),
    (
        "disk",
        "size_after_optimize",
        "size_mb",
        "Size on store after compaction (MB)",
    ),
    (
        "startup",
        "after_optimize",
        "wall_s",
        "Process-cold start-up after compaction (s)",
    ),
    (
        "series",
        "after_optimize",
        "full_p50_ms",
        "Full series after compaction, p50 (ms)",
    ),
    (
        "series",
        "after_optimize",
        "full_p99_ms",
        "Full series after compaction, p99 (ms)",
    ),
    (
        "series",
        "after_optimize",
        "window_p50_ms",
        "100-step window after compaction, p50 (ms)",
    ),
    (
        "series",
        "after_optimize",
        "window_p99_ms",
        "100-step window after compaction, p99 (ms)",
    ),
    (
        "records_key_write",
        "",
        "write_s",
        "Records: write 2.5 x N rows, half rewritten (s)",
    ),
    (
        "records_key_read",
        "",
        "lookup_first_ms",
        "Lookup by item id (filtered scan), first (ms)",
    ),
    (
        "records_key_read",
        "",
        "lookup_p50_ms",
        "Lookup by item id (filtered scan), p50 (ms)",
    ),
    (
        "records_key_read",
        "",
        "lookup_p99_ms",
        "Lookup by item id (filtered scan), p99 (ms)",
    ),
    (
        "records_key_read",
        "",
        "requests_per_lookup",
        "Lookup by item id: requests",
    ),
    (
        "records_key_read",
        "",
        "get_p50_ms",
        "Lookup by primary key, p50 (ms)",
    ),
    (
        "records_key_read",
        "",
        "get_p99_ms",
        "Lookup by primary key, p99 (ms)",
    ),
    (
        "records_key_read",
        "",
        "scan_s",
        "Records: full read, JSON parsed (s)",
    ),
    (
        "optimize",
        "records_key",
        "wall_s",
        "Compaction of the records tables (s)",
    ),
    (
        "records_key_read",
        "after_optimize",
        "lookup_p50_ms",
        "Lookup by item id after compaction, p50 (ms)",
    ),
    (
        "records_key_read",
        "after_optimize",
        "lookup_p99_ms",
        "Lookup by item id after compaction, p99 (ms)",
    ),
    (
        "records_key_read",
        "after_optimize",
        "scan_s",
        "Records: full read after compaction (s)",
    ),
    (
        "records_cluster_write",
        "",
        "write_s",
        "Cluster key: write (s)",
    ),
    (
        "records_cluster_read",
        "",
        "per_run_first_ms",
        "Cluster key: first run-id query (ms)",
    ),
    (
        "records_cluster_read",
        "",
        "per_run_queries_ms",
        "Cluster key: 31 run-id queries (ms)",
    ),
    (
        "records_cluster_read",
        "",
        "per_run_p50_ms",
        "Cluster key: one run-id query, p50 (ms)",
    ),
    (
        "records_cluster_read",
        "",
        "per_run_p99_ms",
        "Cluster key: one run-id query, p99 (ms)",
    ),
    (
        "records_cluster_read",
        "",
        "in_query_ms",
        "Cluster key: one IN query of 31 run ids (ms)",
    ),
    (
        "optimize",
        "records_cluster",
        "wall_s",
        "Compaction of the cluster-key table (s)",
    ),
    (
        "records_cluster_read",
        "after_optimize",
        "per_run_p50_ms",
        "Cluster key after compaction: one run-id query, p50 (ms)",
    ),
    (
        "records_cluster_read",
        "after_optimize",
        "in_query_ms",
        "Cluster key after compaction: IN query (ms)",
    ),
];

const MW_ROWS: &[(&str, &str)] = &[
    ("commits_per_s", "commits per second"),
    ("commit_p50_ms", "commit p50 (ms)"),
    ("commit_p99_ms", "commit p99 (ms)"),
    ("requests_per_commit", "requests per commit"),
    ("wall_s", "wall time (s)"),
];

const COLUMNS: &[(&str, &str)] = &[
    ("lance", "Lance"),
    ("delta", "Delta"),
    ("moraine", "moraine"),
    ("moraine-own", "moraine, own layout"),
    ("moraine-eventual", "moraine, eventual"),
];

fn fmt_num(x: f64) -> String {
    let a = x.abs();
    if a >= 1000.0 {
        format!("{x:.0}")
    } else if a >= 100.0 {
        format!("{x:.1}")
    } else {
        format!("{x:.2}")
    }
}

/// The median of the ok lines' values, with the range when there is more than one; "failed"
/// counts are noted.
fn cell(hits: &[&Value], metric: &str) -> String {
    if hits.is_empty() {
        return "-".into();
    }
    if hits.iter().all(|l| l["status"] == "not_implemented") {
        return "not yet implemented".into();
    }
    let failed = hits.iter().filter(|l| l["status"] == "error").count();
    let mut v: Vec<f64> = hits
        .iter()
        .filter(|l| l["status"] == "ok")
        .filter_map(|l| l["metrics"][metric].as_f64())
        .collect();
    let any_ok = hits.iter().any(|l| l["status"] == "ok");
    let mut out = if v.is_empty() {
        if failed > 0 && !any_ok {
            "failed".into()
        } else {
            return "-".into();
        }
    } else {
        v.sort_by(|a, b| a.total_cmp(b));
        let mid = v[(v.len() - 1) / 2] * 0.5 + v[v.len() / 2] * 0.5;
        if v.len() == 1 {
            fmt_num(mid)
        } else {
            format!(
                "{} ({}-{})",
                fmt_num(mid),
                fmt_num(v[0]),
                fmt_num(v[v.len() - 1])
            )
        }
    };
    if failed > 0 && out != "failed" {
        out.push_str(&format!(", {failed} failed"));
    }
    out
}

fn report(files: &[String]) {
    let mut lines: Vec<Value> = Vec::new();
    for f in files {
        let text = std::fs::read_to_string(f).unwrap_or_else(|e| panic!("{f}: {e}"));
        lines.extend(text.lines().filter_map(|l| serde_json::from_str(l).ok()));
    }
    let mut groups: Vec<(String, String)> = Vec::new();
    for l in &lines {
        let g = (
            l["backend"].as_str().unwrap_or("").to_string(),
            l["scale"].as_str().unwrap_or("").to_string(),
        );
        if !groups.contains(&g) {
            groups.push(g);
        }
    }
    for (backend, scale) in groups {
        let matching = |e: &str, workload: &str, label: &str| -> Vec<&Value> {
            lines
                .iter()
                .filter(|l| {
                    l["engine"] == e
                        && l["backend"] == backend.as_str()
                        && l["scale"] == scale.as_str()
                        && l["workload"] == workload
                        && l["label"].as_str().unwrap_or("") == label
                })
                .collect()
        };
        let present: Vec<&(&str, &str)> = COLUMNS
            .iter()
            .filter(|(e, _)| lines.iter().any(|l| l["engine"] == *e))
            .collect();
        let reps = lines
            .iter()
            .filter_map(|l| l["rep"].as_u64())
            .max()
            .map_or(1, |r| r + 1);
        println!("### {backend}, scale {scale} (median of {reps}, range in brackets)\n");
        let heads: Vec<&str> = present.iter().map(|c| c.1).collect();
        println!("| Measure | {} |", heads.join(" | "));
        println!("|---|{}", "---|".repeat(present.len()));
        for (workload, label, metric, heading) in ROWS {
            let cells: Vec<String> = present
                .iter()
                .map(|(e, _)| cell(&matching(e, workload, label), metric))
                .collect();
            if cells.iter().all(|c| c == "-" || c == "not yet implemented") {
                continue;
            }
            println!("| {heading} | {} |", cells.join(" | "));
        }
        println!();

        // Multi-writer, by writer count.
        let mut counts: Vec<u64> = lines
            .iter()
            .filter(|l| l["workload"] == "multi_writer" && l["backend"] == backend.as_str())
            .filter_map(|l| l["metrics"]["writers"].as_u64())
            .collect();
        counts.sort_unstable();
        counts.dedup();
        if counts.is_empty() {
            continue;
        }
        let mw: Vec<&(&str, &str)> = present
            .iter()
            .copied()
            .filter(|(e, _)| {
                lines
                    .iter()
                    .any(|l| l["engine"] == *e && l["workload"] == "multi_writer")
            })
            .collect();
        println!(
            "| Multi-writer | {} |",
            mw.iter().map(|c| c.1).collect::<Vec<_>>().join(" | ")
        );
        println!("|---|{}", "---|".repeat(mw.len()));
        for n in &counts {
            let label = format!("w{n}");
            for (metric, heading) in MW_ROWS {
                let cells: Vec<String> = mw
                    .iter()
                    .map(|(e, _)| cell(&matching(e, "multi_writer", &label), metric))
                    .collect();
                if cells.iter().all(|c| c == "-") {
                    continue;
                }
                println!("| {n} writers: {heading} | {} |", cells.join(" | "));
            }
            let verified: Vec<String> = mw
                .iter()
                .map(|(e, _)| {
                    let v = matching(e, "mw_verify", &label);
                    let ok = v.iter().filter(|l| l["status"] == "ok").count();
                    format!("{ok} of {}", v.len())
                })
                .collect();
            println!(
                "| {n} writers: runs with every row once | {} |",
                verified.join(" | ")
            );
        }
        println!();
    }
}
