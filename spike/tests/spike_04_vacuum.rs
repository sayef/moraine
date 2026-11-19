//! Spike item 4: does vacuum accept a retention under 168 hours, does it delete only inside the
//! table root, and where does it take a file's age from?
//!
//! Layout: `db/t/data` is the Delta table; `db/t/idx`, `db/t/blobs` and `db/t/meta` are siblings
//! holding objects vacuum must never touch.

use std::sync::Arc;

use deltalake::datafusion::prelude::{col, lit};
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{Action, Remove};
use deltalake::operations::vacuum::{Clock, VacuumMode};
use deltalake::protocol::DeltaOperation;
use futures::TryStreamExt;
use moraine_spike::*;
use object_store::ObjectStoreExt;
use object_store::path::Path;

#[derive(Debug)]
struct FixedClock(i64);
impl Clock for FixedClock {
    fn current_timestamp_millis(&self) -> i64 {
        self.0
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

async fn vacuum(
    backend: &Backend,
    rel: &str,
    mode: VacuumMode,
    retention_h: i64,
    enforce: bool,
    clock_ms: Option<i64>,
) -> Result<Vec<String>, String> {
    let table = backend.open(rel).await.unwrap();
    let mut b = table
        .vacuum()
        .with_retention_period(chrono::Duration::hours(retention_h))
        .with_mode(mode)
        .with_enforce_retention_duration(enforce);
    if let Some(ms) = clock_ms {
        b = b.with_clock(Arc::new(FixedClock(ms)));
    }
    b.await
        .map(|(_, m)| m.files_deleted)
        .map_err(|e| e.to_string())
}

fn short(v: &Result<Vec<String>, String>) -> String {
    match v {
        Ok(files) => format!(
            "deleted {} {:?}",
            files.len(),
            files
                .iter()
                .map(|f| f
                    .rsplit('/')
                    .next()
                    .unwrap_or(f)
                    .chars()
                    .take(18)
                    .collect::<String>())
                .collect::<Vec<_>>()
        ),
        Err(e) => format!("error: {}", e.chars().take(140).collect::<String>()),
    }
}

async fn run(name: &str, backend: Backend) {
    let (store, root) = backend.store().unwrap();
    let p = |s: &str| Path::from(join(&root, s));

    // Table A uses Delta's default deleted-file retention (7 days).
    let rel_default = "db/a/data";
    create_spike_table(&backend, rel_default, &[])
        .await
        .unwrap();
    let r_default = vacuum(&backend, rel_default, VacuumMode::Lite, 1, true, None).await;

    // Table T sets deletedFileRetentionDuration to 1 hour, as the design does.
    let rel = "db/t/data";
    let table = create_spike_table(
        &backend,
        rel,
        &[("delta.deletedFileRetentionDuration", "interval 1 hours")],
    )
    .await
    .unwrap();
    let r_enforced_1h = vacuum(&backend, rel, VacuumMode::Lite, 1, true, None).await;

    let siblings = [
        "db/t/idx/part-x.parquet/emb.0123456789abcdef.osix",
        "db/t/blobs/01/0190-blob",
        "db/t/meta/leases/vacuum.json",
        "db/t-other/data/part-foreign.parquet",
    ];
    for s in siblings {
        store.put(&p(s), vec![1u8; 64].into()).await.unwrap();
    }
    // Hidden and underscore-prefixed objects inside the table root.
    for s in ["db/t/data/_moraine/marker", "db/t/data/.hidden"] {
        store.put(&p(s), vec![1u8; 8].into()).await.unwrap();
    }

    // Three data files: seq 0, 1 and 2.
    let mut table = table;
    for seq in 0..3 {
        table = table
            .write(vec![spike_batch(0, seq, 5, seq)])
            .await
            .unwrap();
    }
    // Delete seq 0: its file becomes a tombstone stamped with this client's clock.
    let (t, _) = table
        .delete()
        .with_predicate(col("seq").eq(lit(0i64)))
        .await
        .unwrap();
    table = t;
    // Remove seq 1's file by hand with a tombstone timestamp two hours in the past, as a client
    // whose clock runs two hours behind would write it.
    let file_seq1 = table
        .snapshot()
        .unwrap()
        .log_data()
        .iter()
        .find(|f| f.stats().unwrap_or_default().contains("\"seq\":1"))
        .map(|f| (f.path().to_string(), f.size()))
        .unwrap();
    let remove = Remove {
        path: file_seq1.0.clone(),
        data_change: true,
        deletion_timestamp: Some(now_ms() - 2 * 3600 * 1000),
        size: Some(file_seq1.1),
        ..Default::default()
    };
    CommitBuilder::from(CommitProperties::default())
        .with_actions(vec![Action::Remove(remove)])
        .build(
            Some(table.snapshot().unwrap()),
            table.log_store(),
            DeltaOperation::Delete { predicate: None },
        )
        .await
        .unwrap();
    // An orphan: uploaded, never committed (a writer that died before its commit).
    store
        .put(
            &p("db/t/data/part-99999-orphan.parquet"),
            vec![0u8; 128].into(),
        )
        .await
        .unwrap();

    let lite_now = vacuum(&backend, rel, VacuumMode::Lite, 1, true, None).await;
    let full_now = vacuum(&backend, rel, VacuumMode::Full, 1, true, None).await;
    let lite_later = vacuum(
        &backend,
        rel,
        VacuumMode::Lite,
        1,
        true,
        Some(now_ms() + 2 * 3600 * 1000),
    )
    .await;
    let full_later = vacuum(
        &backend,
        rel,
        VacuumMode::Full,
        1,
        true,
        Some(now_ms() + 2 * 3600 * 1000),
    )
    .await;

    let all: Vec<String> = store
        .list(Some(&root))
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await
        .unwrap();
    let siblings_intact = siblings.iter().all(|s| all.iter().any(|a| a.ends_with(s)));
    let hidden_intact = ["_moraine/marker", ".hidden"]
        .iter()
        .all(|s| all.iter().any(|a| a.ends_with(s)));
    let rows: usize = scan_all(&backend, rel)
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();

    println!(
        "RESULT item4 backend={name} default_table_1h_enforced: {}",
        short(&r_default)
    );
    println!(
        "RESULT item4 backend={name} table_1h_property_1h_enforced: {}",
        short(&r_enforced_1h)
    );
    println!(
        "RESULT item4 backend={name} lite_now (seq1 tombstone backdated 2h, seq0 fresh): {}",
        short(&lite_now)
    );
    println!(
        "RESULT item4 backend={name} full_now (orphan just uploaded): {}",
        short(&full_now)
    );
    println!(
        "RESULT item4 backend={name} lite_clock_plus_2h: {}",
        short(&lite_later)
    );
    println!(
        "RESULT item4 backend={name} full_clock_plus_2h: {}",
        short(&full_later)
    );
    println!(
        "RESULT item4 backend={name} siblings_intact={siblings_intact} hidden_in_root_intact={hidden_intact} live_rows={rows} (expect 5)"
    );
    assert!(siblings_intact);
    assert_eq!(rows, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn item4_memory() {
    run("memory", Backend::memory()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn item4_local() {
    let dir = tempfile::tempdir().unwrap();
    run("local", Backend::Local(dir.path().into())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn item4_s3_compatible() {
    let Some(b) = Backend::s3_compatible(&run_prefix("item4")) else {
        println!("RESULT item4 backend=s3 not run (MORAINE_SPIKE_S3_ENDPOINT not set)");
        return;
    };
    run("s3-rustfs", b.clone()).await;
    b.clear().await.unwrap();
}
