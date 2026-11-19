//! Spike item 1: delta-rs commits safely through put-if-absent alone, without DynamoDB.
//! 8 writers, 200 appends each, 10 rows per append. Every row must be present and the log must
//! hold every version from 0 to 1,600 with no gaps.
//!
//! Local and S3 runs use 8 separate processes (the `appender` binary); the in-memory run uses
//! 8 tasks with separate table handles, since memory cannot be shared across processes.

use moraine_spike::*;

const WRITERS: i64 = 8;
const COUNT: i64 = 200;
const ROWS: usize = 10;

async fn run(name: &str, backend: Backend, processes: bool) {
    let rel = "db/t/data";
    create_spike_table(&backend, rel, &[]).await.unwrap();
    let (lines, secs) = if processes {
        run_appender_processes(
            env!("CARGO_BIN_EXE_appender"),
            &backend,
            rel,
            WRITERS,
            COUNT,
            ROWS,
        )
        .await
        .unwrap()
    } else {
        run_appender_tasks(&backend, rel, WRITERS, COUNT, ROWS)
            .await
            .unwrap()
    };
    for l in &lines {
        println!("WRITER item1 backend={name} {l}");
    }
    let c = check_appends(&backend, rel, WRITERS, COUNT, ROWS, 1)
        .await
        .unwrap();
    let commits = (WRITERS * COUNT) as f64;
    println!(
        "RESULT item1 backend={name} processes={processes} wall_s={secs:.1} commits_per_s={:.1} \
         rows={}/{} latest_version={} log_files={} gapless={} bad_groups={} txn_ok={} \
         delta_rs_retries={} duplicate_txn={}",
        commits / secs,
        c.rows,
        c.expected_rows,
        c.latest_version,
        c.log_versions_present,
        c.gapless,
        c.bad_groups,
        c.txn_ok,
        c.total_retries,
        c.duplicate_txn_commits
    );
    assert_eq!(c.rows, c.expected_rows);
    assert_eq!(c.latest_version, (WRITERS * COUNT) as u64);
    assert!(c.gapless);
    assert_eq!(c.bad_groups, 0);
    assert!(c.txn_ok);
    assert_eq!(c.duplicate_txn_commits, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn item1_memory_tasks() {
    run("memory", Backend::memory(), false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn item1_local_processes() {
    let dir = tempfile::tempdir().unwrap();
    run("local", Backend::Local(dir.path().into()), true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn item1_s3_compatible_processes() {
    let Some(b) = Backend::s3_compatible(&run_prefix("item1")) else {
        println!("RESULT item1 backend=s3 not run (MORAINE_SPIKE_S3_ENDPOINT not set)");
        return;
    };
    run("s3-rustfs", b.clone(), true).await;
    b.clear().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a real bucket: set MORAINE_SPIKE_REAL_S3_BUCKET and AWS credentials"]
async fn item1_real_s3_processes() {
    let b = Backend::real_s3(&run_prefix("item1")).expect("MORAINE_SPIKE_REAL_S3_BUCKET");
    run("s3-aws", b.clone(), true).await;
    b.clear().await.unwrap();
}
