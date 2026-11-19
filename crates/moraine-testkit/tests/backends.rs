//! T6: the public API scenario on real backends. Local disk always runs (case
//! S11); the S3-compatible server from `spike/s3-server.sh` runs when its variables are set
//! (`source <(spike/s3-server.sh env)`), and real S3 (T7) when `MORAINE_T7_S3_LOCATION` is.
//! Concurrent writers from separate handles, a creation race, refresh, scans, lookups, and
//! D18's request budget measured on the wire. Every handle writes with object tags (D29), so
//! the budget also shows that tags cost no request.

use std::collections::HashMap;

use moraine::expr::{ScalarValue, col, lit};
use moraine::store::Op;
use moraine::{Consistency, Database, Error, TableOptions};
use moraine_testkit::SimRng;
use moraine_testkit::datagen;

async fn db(location: &str, options: &HashMap<String, String>) -> Database {
    Database::builder(location)
        .storage_options(options.clone())
        .consistency(Consistency::Strong)
        .object_tags([("suite", "backends"), ("env", "test")])
        .open()
        .await
        .unwrap()
}

async fn suite(location: &str, options: HashMap<String, String>) {
    let opts = TableOptions::default()
        .primary_key(["k"])
        .enforce_primary_key(false)
        .cluster_by(["g"]);
    // Creation race: four handles, identical definitions, one table.
    let handles: Vec<Database> =
        futures::future::join_all((0..4).map(|_| db(location, &options))).await;
    let tables = futures::future::join_all(
        handles
            .iter()
            .map(|d| d.create_table("events", datagen::row_schema(), opts.clone())),
    )
    .await;
    let tables: Vec<_> = tables.into_iter().map(|t| t.unwrap()).collect();
    // A conflicting definition is refused.
    assert!(matches!(
        handles[0]
            .create_table("events", datagen::row_schema(), TableOptions::default())
            .await,
        Err(Error::TableExistsWithDifferentSchema(_))
    ));

    // Four writers, ten appends each, concurrently.
    let mut tasks = Vec::new();
    for (w, t) in tables.iter().enumerate() {
        let t = t.clone();
        tasks.push(tokio::spawn(async move {
            let mut rng = SimRng::new(w as u64);
            let mut versions = Vec::new();
            for a in 0..10 {
                let b = datagen::rows(&mut rng, &format!("w{w}a{a}"), 5, None, 3);
                versions.push(t.append(vec![b]).await.unwrap().version);
            }
            versions
        }));
    }
    let mut versions: Vec<u64> = Vec::new();
    for t in tasks {
        versions.extend(t.await.unwrap());
    }
    versions.sort_unstable();
    versions.dedup();
    let t = &tables[0];
    let latest = t.refresh().await.unwrap();
    assert_eq!(latest, *versions.last().unwrap());
    assert_eq!(t.scan().count().await.unwrap(), 4 * 10 * 5);

    // Filters and lookups.
    let g1 = t
        .scan()
        .filter(col("g").eq(lit(1i64)))
        .count()
        .await
        .unwrap();
    let g_all = t
        .scan()
        .filter(col("g").is_not_null())
        .count()
        .await
        .unwrap();
    assert!(g1 > 0 && g1 < g_all);
    let snap = t.snapshot().await.unwrap();
    let rows = snap.scan().select(["k"]).limit(3).collect().await.unwrap();
    let k = arrow::array::AsArray::as_string::<i32>(rows[0].column(0))
        .value(0)
        .to_string();
    assert!(
        snap.get(&[ScalarValue::Utf8(Some(k))])
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        snap.get(&[ScalarValue::Utf8(Some("no such key".into()))])
            .await
            .unwrap()
            .is_none()
    );

    // D18 on the wire: an append with a current snapshot is two requests, a refresh one.
    let before = handles[0].store_metrics().snapshot();
    let mut rng = SimRng::new(99);
    t.append(vec![datagen::rows(&mut rng, "last", 3, None, 3)])
        .await
        .unwrap();
    t.refresh().await.unwrap();
    let d = handles[0].store_metrics().snapshot().since(&before);
    assert_eq!(d.count(Op::PutOverwrite), 1, "{d}");
    assert_eq!(d.count(Op::PutCreate), 1, "{d}");
    assert_eq!(d.count(Op::Get), 1, "{d}");
    assert_eq!(d.requests(), 3, "{d}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_disk() {
    let dir = tempfile::tempdir().unwrap();
    suite(dir.path().to_str().unwrap(), HashMap::new()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_compatible_server() {
    let Ok(endpoint) = std::env::var("MORAINE_SPIKE_S3_ENDPOINT") else {
        println!("SKIP: MORAINE_SPIKE_S3_ENDPOINT not set");
        return;
    };
    let bucket =
        std::env::var("MORAINE_SPIKE_S3_BUCKET").unwrap_or_else(|_| "moraine-spike".into());
    let mut o = HashMap::new();
    o.insert("aws_endpoint".to_string(), endpoint);
    o.insert("aws_allow_http".to_string(), "true".to_string());
    o.insert("aws_region".to_string(), "us-east-1".to_string());
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let location = format!("s3://{bucket}/t6-{stamp:x}");
    suite(&location, o).await;
}

/// T7 on real S3 (`.github/workflows/cloud-nightly.yml`): the same suite under a fresh prefix of
/// `MORAINE_T7_S3_LOCATION` (`s3://<bucket>/<prefix>`), with credentials and region from the
/// environment (the AWS SDK's variables, as OIDC sets them). Skipped when the variable is unset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_s3() {
    let Ok(base) = std::env::var("MORAINE_T7_S3_LOCATION") else {
        println!("SKIP: MORAINE_T7_S3_LOCATION not set");
        return;
    };
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let location = format!("{}/t7-{stamp:x}", base.trim_end_matches('/'));
    suite(&location, HashMap::new()).await;
}

/// Profiling aid for the multi-writer tail on the S3-compatible server (not part of the suite):
/// 8 handles x 20 appends at once, printing latency percentiles and, per request kind, how many
/// requests failed. `cargo test --test backends s3_contention_profile -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_contention_profile() {
    let Ok(endpoint) = std::env::var("MORAINE_SPIKE_S3_ENDPOINT") else {
        return;
    };
    let bucket =
        std::env::var("MORAINE_SPIKE_S3_BUCKET").unwrap_or_else(|_| "moraine-spike".into());
    let o: HashMap<String, String> = [
        ("aws_endpoint", endpoint.as_str()),
        ("aws_allow_http", "true"),
        ("aws_region", "us-east-1"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let location = format!("s3://{bucket}/prof-{stamp:x}");
    let first = db(&location, &o).await;
    first
        .create_table("events", datagen::row_schema(), TableOptions::default())
        .await
        .unwrap();
    let mut dbs = Vec::new();
    for _ in 0..8 {
        let d = db(&location, &o).await;
        let t = d.open_table("events").await.unwrap();
        let mut rng = SimRng::new(1);
        t.append(vec![datagen::rows(&mut rng, "warm", 1, None, 2)])
            .await
            .unwrap();
        dbs.push((d, t));
    }
    let before: Vec<_> = dbs
        .iter()
        .map(|(d, _)| d.store_metrics().snapshot())
        .collect();
    let tasks: Vec<_> = dbs
        .iter()
        .enumerate()
        .map(|(w, (_, t))| {
            let t = t.clone();
            tokio::spawn(async move {
                let mut rng = SimRng::new(w as u64);
                let mut lat = Vec::new();
                for a in 0..20 {
                    let b = datagen::rows(&mut rng, &format!("w{w}a{a}"), 100, None, 2);
                    let t0 = std::time::Instant::now();
                    t.append(vec![b]).await.unwrap();
                    lat.push(t0.elapsed().as_secs_f64() * 1e3);
                }
                lat
            })
        })
        .collect();
    let mut lat = Vec::new();
    for t in tasks {
        lat.extend(t.await.unwrap());
    }
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |q: f64| lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)];
    println!(
        "PROFILE p50={:.1}ms p90={:.1}ms p99={:.1}ms max={:.1}ms",
        p(0.5),
        p(0.9),
        p(0.99),
        lat[lat.len() - 1]
    );
    let mut total: std::collections::BTreeMap<&str, (u64, u64)> = Default::default();
    for (i, (d, _)) in dbs.iter().enumerate() {
        for (op, c) in d.store_metrics().snapshot().since(&before[i]).by_op {
            let e = total.entry(op.name()).or_default();
            e.0 += c.requests;
            e.1 += c.errors;
        }
    }
    println!("PROFILE requests (sent, failed) per kind for 160 commits: {total:?}");
}
