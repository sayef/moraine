//! Spike item 5: checkpoints at `delta.checkpointInterval`, expired commit files removed under
//! `delta.logRetentionDuration`, and two clients checkpointing the same version.
//!
//! 4 writers x 125 appends = 500 commits, checkpoint interval 20, log retention 2 seconds (so
//! cleanup is observable inside a test; the design uses the grace period).

use moraine_spike::*;
use object_store::ObjectStoreExt;
use object_store::path::Path;

const WRITERS: i64 = 4;
const COUNT: i64 = 125;
const ROWS: usize = 4;

async fn run(name: &str, backend: Backend, processes: bool) {
    let rel = "db/t/data";
    let config = [
        ("delta.checkpointInterval", "20"),
        ("delta.logRetentionDuration", "interval 2 seconds"),
        ("delta.enableExpiredLogCleanup", "true"),
    ];
    create_spike_table(&backend, rel, &config).await.unwrap();
    let (_, secs) = if processes {
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
    let (commits, checkpoints, other) = list_log(&backend, rel).await.unwrap();
    let c = check_appends(&backend, rel, WRITERS, COUNT, ROWS, 0)
        .await
        .unwrap();
    let (store, root) = backend.store().unwrap();
    let last_cp = store
        .get(&Path::from(format!(
            "{}/_delta_log/_last_checkpoint",
            join(&root, rel)
        )))
        .await
        .ok();
    let last_cp = match last_cp {
        Some(g) => String::from_utf8_lossy(&g.bytes().await.unwrap())
            .chars()
            .take(120)
            .collect::<String>(),
        None => "missing".into(),
    };
    let not_multiple: Vec<_> = checkpoints.iter().filter(|v| **v % 20 != 0).collect();
    println!(
        "RESULT item5 backend={name} processes={processes} wall_s={secs:.1} latest={} commit_files_left={} \
         first_commit_left={:?} checkpoints={} checkpoint_versions_not_multiple_of_20={not_multiple:?} \
         last_checkpoint={last_cp} other_log_files={} rows={}/{} txn_ok={}",
        c.latest_version,
        commits.len(),
        commits.first(),
        checkpoints.len(),
        other.len(),
        c.rows,
        c.expected_rows,
        c.txn_ok
    );
    assert_eq!(c.rows, c.expected_rows);
    assert!(c.txn_ok);

    // Two clients checkpoint the same version: first one after the other, comparing bytes,
    // then both at once.
    let a = backend.open(rel).await.unwrap();
    let b = backend.open(rel).await.unwrap();
    let v = a.version().unwrap();
    let cp_path = Path::from(format!(
        "{}/_delta_log/{v:020}.checkpoint.parquet",
        join(&root, rel)
    ));
    deltalake::checkpoints::create_checkpoint(&a, None)
        .await
        .unwrap();
    let first = store.get(&cp_path).await.unwrap().bytes().await.unwrap();
    deltalake::checkpoints::create_checkpoint(&b, None)
        .await
        .unwrap();
    let second = store.get(&cp_path).await.unwrap().bytes().await.unwrap();
    let (ra, rb) = tokio::join!(
        deltalake::checkpoints::create_checkpoint(&a, None),
        deltalake::checkpoints::create_checkpoint(&b, None)
    );
    let reload = scan_all(&backend, rel)
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    println!(
        "RESULT item5 backend={name} same_version_checkpoint: bytes_equal={} sizes=({}, {}) concurrent=({}, {}) rows_after_reload={reload}",
        first == second,
        first.len(),
        second.len(),
        ra.is_ok(),
        rb.is_ok()
    );
    assert_eq!(reload, c.expected_rows);
}

#[tokio::test(flavor = "multi_thread")]
async fn item5_memory_tasks() {
    run("memory", Backend::memory(), false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn item5_local_processes() {
    let dir = tempfile::tempdir().unwrap();
    run("local", Backend::Local(dir.path().into()), true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn item5_s3_compatible_processes() {
    let Some(b) = Backend::s3_compatible(&run_prefix("item5")) else {
        println!("RESULT item5 backend=s3 not run (MORAINE_SPIKE_S3_ENDPOINT not set)");
        return;
    };
    run("s3-rustfs", b.clone(), true).await;
    b.clear().await.unwrap();
}
