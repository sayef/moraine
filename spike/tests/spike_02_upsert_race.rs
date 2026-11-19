//! Spike item 2: does the delta-rs conflict checker fail a merge when a concurrent commit added
//! rows the merge would have matched?
//!
//! Each round uses a new key. Two writers, each with its own table handle on the same store,
//! load the latest snapshot, meet at a barrier, and then write that key at the same moment. A
//! writer whose commit fails with a conflict reloads and runs its operation again, as the design's
//! write path would. At the end every key must have exactly one row.
//!
//! Variants:
//! - `merge_vs_merge`: both writers upsert (merge) the key. The design's scenario.
//! - `append_vs_merge`: one writer appends the key blindly, the other upserts it.
//! - each of the above with `delta.isolationLevel = Serializable`.
//! - `merge_vs_merge_no_retries`: the design's fallback, merges with commit retries set to zero.

use std::sync::Arc;
use std::time::Instant;

use deltalake::DeltaTable;
use deltalake::datafusion::prelude::{SessionContext, col};
use deltalake::kernel::transaction::CommitProperties;
use moraine_spike::*;
use tokio::sync::Barrier;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Op {
    Merge,
    Append,
}

async fn upsert(
    table: DeltaTable,
    writer: i64,
    round: i64,
    key: &str,
    max_retries: usize,
) -> deltalake::DeltaResult<DeltaTable> {
    let ctx = SessionContext::new();
    let source = ctx
        .read_batch(key_batch(writer, round, key, writer))
        .unwrap();
    let (t, _) = table
        .merge(source, col("target.key").eq(col("source.key")))
        .with_source_alias("source")
        .with_target_alias("target")
        .with_commit_properties(CommitProperties::default().with_max_retries(max_retries))
        .when_matched_update(|u| {
            u.update("writer", col("source.writer"))
                .update("seq", col("source.seq"))
                .update("value", col("source.value"))
        })?
        .when_not_matched_insert(|i| {
            i.set("writer", col("source.writer"))
                .set("seq", col("source.seq"))
                .set("key", col("source.key"))
                .set("value", col("source.value"))
        })?
        .await?;
    Ok(t)
}

async fn append(
    table: DeltaTable,
    writer: i64,
    round: i64,
    key: &str,
) -> deltalake::DeltaResult<DeltaTable> {
    table
        .write(vec![key_batch(writer, round, key, writer)])
        .await
}

struct Outcome {
    conflicts: u64,
}

async fn writer_loop(
    backend: Backend,
    rel: String,
    writer: i64,
    op: Op,
    rounds: i64,
    max_retries: usize,
    barrier: Arc<Barrier>,
) -> Outcome {
    let mut table = backend.open(&rel).await.unwrap();
    let mut conflicts = 0;
    for round in 0..rounds {
        let key = format!("round-{round}");
        table.update_state().await.unwrap();
        barrier.wait().await;
        loop {
            let res = match op {
                Op::Merge => upsert(table.clone(), writer, round, &key, max_retries).await,
                Op::Append => append(table.clone(), writer, round, &key).await,
            };
            match res {
                Ok(t) => {
                    table = t;
                    break;
                }
                Err(e) => {
                    let msg = e.to_string();
                    assert!(
                        {
                            let m = msg.to_lowercase();
                            m.contains("conflict") || m.contains("commit") || m.contains("attempt")
                        },
                        "unexpected error: {msg}"
                    );
                    conflicts += 1;
                    table.update_state().await.unwrap();
                }
            }
        }
        // Keep the writers in lock step so every round is a fresh race.
        barrier.wait().await;
    }
    Outcome { conflicts }
}

async fn run(
    name: &str,
    backend: Backend,
    variant: &str,
    ops: [Op; 2],
    isolation: Option<&str>,
    rounds: i64,
    max_retries: usize,
) {
    let rel = format!("db/{variant}/data");
    let config: Vec<(&str, &str)> = isolation
        .map(|i| vec![("delta.isolationLevel", i)])
        .unwrap_or_default();
    create_spike_table(&backend, &rel, &config).await.unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let start = Instant::now();
    let mut tasks = vec![];
    for (w, op) in ops.into_iter().enumerate() {
        tasks.push(tokio::spawn(writer_loop(
            backend.clone(),
            rel.clone(),
            w as i64,
            op,
            rounds,
            max_retries,
            barrier.clone(),
        )));
    }
    let mut conflicts = vec![];
    for t in tasks {
        conflicts.push(t.await.unwrap().conflicts);
    }
    let secs = start.elapsed().as_secs_f64();
    let batches = scan_all(&backend, &rel).await.unwrap();
    let (_, by_key) = tally(&batches);
    let keys = by_key.len();
    let dup_keys = by_key.values().filter(|n| **n > 1).count();
    let max_per_key = by_key.values().copied().max().unwrap_or(0);
    // Show what the last merge recorded as its read predicate, if any.
    let table = backend.open(&rel).await.unwrap();
    let last = table.version().unwrap() as u64;
    let mut predicate = String::from("-");
    for v in (1..=last).rev().take(4) {
        for a in read_commit(&backend, &rel, v).await.unwrap() {
            if a.pointer("/commitInfo/operation").and_then(|o| o.as_str()) == Some("MERGE") {
                predicate = a
                    .pointer("/commitInfo/operationParameters/predicate")
                    .map(|p| p.to_string())
                    .unwrap_or_else(|| "none".into());
            }
        }
        if predicate != "-" {
            break;
        }
    }
    println!(
        "RESULT item2 backend={name} variant={variant} ops={ops:?} isolation={} rounds={rounds} \
         keys={keys} keys_with_duplicates={dup_keys} max_rows_per_key={max_per_key} \
         conflicts_retried={conflicts:?} versions={last} wall_s={secs:.1} merge_read_predicate={predicate}",
        isolation.unwrap_or("default(WriteSerializable)")
    );
}

const MERGE2: [Op; 2] = [Op::Merge, Op::Merge];
const MIXED: [Op; 2] = [Op::Append, Op::Merge];

#[tokio::test(flavor = "multi_thread")]
async fn item2_memory() {
    let b = Backend::memory();
    run(
        "memory",
        b.clone(),
        "merge_vs_merge",
        MERGE2,
        None,
        1000,
        15,
    )
    .await;
    run(
        "memory",
        b.clone(),
        "merge_vs_merge_serializable",
        MERGE2,
        Some("Serializable"),
        300,
        15,
    )
    .await;
    run("memory", b.clone(), "append_vs_merge", MIXED, None, 300, 15).await;
    run(
        "memory",
        b.clone(),
        "append_vs_merge_serializable",
        MIXED,
        Some("Serializable"),
        300,
        15,
    )
    .await;
    run(
        "memory",
        b.clone(),
        "merge_vs_merge_no_retries",
        MERGE2,
        None,
        300,
        0,
    )
    .await;
    run(
        "memory",
        b,
        "append_vs_merge_no_retries",
        MIXED,
        None,
        300,
        0,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn item2_local() {
    let dir = tempfile::tempdir().unwrap();
    let b = Backend::Local(dir.path().into());
    run("local", b.clone(), "merge_vs_merge", MERGE2, None, 300, 15).await;
    run("local", b, "append_vs_merge", MIXED, None, 300, 15).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn item2_s3_compatible() {
    let Some(b) = Backend::s3_compatible(&run_prefix("item2")) else {
        println!("RESULT item2 backend=s3 not run (MORAINE_SPIKE_S3_ENDPOINT not set)");
        return;
    };
    run(
        "s3-rustfs",
        b.clone(),
        "merge_vs_merge",
        MERGE2,
        None,
        200,
        15,
    )
    .await;
    run(
        "s3-rustfs",
        b.clone(),
        "append_vs_merge",
        MIXED,
        None,
        200,
        15,
    )
    .await;
    b.clear().await.unwrap();
}
