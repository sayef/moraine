//! Spike items 6, 7 and 11, which all inspect what delta-rs records in the log.
//!
//! - Item 6: a commit carries an application transaction (`txn`) and it can be read back, after a
//!   reopen and after a checkpoint, for appends, merges and deletes; and a commit holding a given
//!   transaction can be found by scanning the log from a base version.
//! - Item 7: custom `moraine.*` table properties are accepted and round-trip; and whether a
//!   property change conflicts with a concurrent append (the design assumes it rebases).
//! - Item 11: the table is created without deletion vectors, and the protocol's table features
//!   are visible so the library can refuse tables that use them.

use std::collections::HashMap;
use std::time::Instant;

use deltalake::datafusion::prelude::{SessionContext, col, lit};
use deltalake::kernel::Transaction;
use deltalake::kernel::transaction::CommitProperties;
use moraine_spike::*;

fn txn(app: &str, v: i64) -> CommitProperties {
    CommitProperties::default().with_application_transaction(Transaction::new(app, v))
}

async fn txn_version(backend: &Backend, rel: &str, app: &str) -> Option<i64> {
    let t = backend.open(rel).await.unwrap();
    t.snapshot()
        .unwrap()
        .transaction_version(t.log_store().as_ref(), app)
        .await
        .unwrap()
}

/// The design's `find_txn`: scan commit files after `since` for a `txn` action.
async fn find_txn(
    backend: &Backend,
    rel: &str,
    app: &str,
    version: i64,
    since: u64,
) -> Option<u64> {
    let (commits, _, _) = list_log(backend, rel).await.unwrap();
    for v in commits.into_iter().filter(|v| *v > since) {
        for a in read_commit(backend, rel, v).await.unwrap() {
            if a.pointer("/txn/appId").and_then(|x| x.as_str()) == Some(app)
                && a.pointer("/txn/version").and_then(|x| x.as_i64()) == Some(version)
            {
                return Some(v);
            }
        }
    }
    None
}

async fn item6(name: &str, backend: &Backend) {
    let rel = "db/txn/data";
    let mut t = create_spike_table(backend, rel, &[]).await.unwrap();
    let base = t.version().unwrap() as u64;
    let writer = uuid::Uuid::new_v4().to_string();
    t = t
        .write(vec![spike_batch(1, 0, 3, 0)])
        .with_commit_properties(txn(&writer, 1))
        .await
        .unwrap();
    let v_append = t.version().unwrap() as u64;
    let ctx = SessionContext::new();
    let src = ctx.read_batch(key_batch(1, 9, "w1-s0-r0", 99)).unwrap();
    let (t2, _) = t
        .merge(src, col("target.key").eq(col("source.key")))
        .with_source_alias("source")
        .with_target_alias("target")
        .with_commit_properties(txn(&writer, 2))
        .when_matched_update(|u| u.update("value", col("source.value")))
        .unwrap()
        .await
        .unwrap();
    let (t3, _) = t2
        .delete()
        .with_predicate(col("key").eq(lit("w1-s0-r1")))
        .with_commit_properties(txn(&writer, 3))
        .await
        .unwrap();
    let after_ops = txn_version(backend, rel, &writer).await;
    deltalake::checkpoints::create_checkpoint(&t3, None)
        .await
        .unwrap();
    let after_cp = txn_version(backend, rel, &writer).await;
    let found = find_txn(backend, rel, &writer, 1, base).await;
    let missing = find_txn(backend, rel, &writer, 4, base).await;

    // Cost of the lookup with many writers in the log: 300 more app ids, one commit each.
    let mut t = backend.open(rel).await.unwrap();
    for i in 0..300 {
        t = t
            .write(vec![spike_batch(2, i, 1, 0)])
            .with_commit_properties(txn(&format!("writer-{i}"), 0))
            .await
            .unwrap();
    }
    let t0 = Instant::now();
    let lookup = txn_version(backend, rel, "writer-150").await;
    let lookup_ms = t0.elapsed().as_secs_f64() * 1e3;
    println!(
        "RESULT item6 backend={name} after_append_merge_delete={after_ops:?} after_checkpoint={after_cp:?} \
         find_txn(v1)={found:?} (append committed at {v_append}) find_txn(absent)={missing:?} \
         lookup_among_301_app_ids={lookup:?} open_and_lookup_ms={lookup_ms:.1}"
    );
    assert_eq!(after_ops, Some(3));
    assert_eq!(after_cp, Some(3));
    assert_eq!(found, Some(v_append));
}

async fn item7(name: &str, backend: &Backend) {
    // Create with custom keys: first with the default key check, then with it off.
    let rel = "db/props/data";
    let indexes = r#"[{"kind":"vector","name":"emb_idx","column":"embedding","metric":"cosine"}]"#;
    let t = backend.open(rel).await.unwrap();
    let strict = t
        .create()
        .with_columns(spike_columns())
        .with_configuration([("moraine.formatVersion", Some("1"))])
        .await
        .map(|_| "ok".to_string())
        .unwrap_or_else(|e| {
            format!(
                "error: {}",
                e.to_string().chars().take(100).collect::<String>()
            )
        });
    let rel = if strict == "ok" {
        "db/props2/data"
    } else {
        rel
    };
    let t = create_spike_table(
        backend,
        rel,
        &[
            ("moraine.formatVersion", "1"),
            ("moraine.indexes", indexes),
            ("moraine.primaryKey", r#"["key"]"#),
        ],
    )
    .await
    .unwrap();
    let reopened = backend.open(rel).await.unwrap();
    let props = reopened
        .snapshot()
        .unwrap()
        .snapshot()
        .table_properties()
        .unknown_properties
        .clone();
    let round_trip = props
        .get("moraine.indexes")
        .map(|s| s == indexes)
        .unwrap_or(false);

    // Change a property with set_tbl_properties.
    let mut change = HashMap::new();
    change.insert("moraine.indexes".to_string(), "[]".to_string());
    let set = t
        .clone()
        .set_tbl_properties()
        .with_properties(change.clone())
        .with_raise_if_not_exists(false)
        .await
        .map(|t| t.version())
        .map_err(|e| e.to_string());
    let after = backend
        .open(rel)
        .await
        .unwrap()
        .snapshot()
        .unwrap()
        .snapshot()
        .table_properties()
        .unknown_properties
        .get("moraine.indexes")
        .cloned();

    // Conflict 1: an append is prepared on version v; a property change commits v+1 first.
    let stale = backend.open(rel).await.unwrap();
    let other = backend.open(rel).await.unwrap();
    let mut c2 = HashMap::new();
    c2.insert("moraine.indexes".to_string(), indexes.to_string());
    other
        .set_tbl_properties()
        .with_properties(c2)
        .with_raise_if_not_exists(false)
        .await
        .unwrap();
    let append_after_meta = stale
        .write(vec![spike_batch(3, 0, 2, 0)])
        .await
        .map(|t| format!("ok v{}", t.version().unwrap()))
        .unwrap_or_else(|e| {
            format!(
                "error: {}",
                e.to_string().chars().take(100).collect::<String>()
            )
        });

    // Conflict 2: a property change is prepared on version v; an append commits v+1 first.
    let stale = backend.open(rel).await.unwrap();
    backend
        .open(rel)
        .await
        .unwrap()
        .write(vec![spike_batch(3, 1, 2, 0)])
        .await
        .unwrap();
    let meta_after_append = stale
        .set_tbl_properties()
        .with_properties(change)
        .with_raise_if_not_exists(false)
        .await
        .map(|t| format!("ok v{}", t.version().unwrap()))
        .unwrap_or_else(|e| {
            format!(
                "error: {}",
                e.to_string().chars().take(100).collect::<String>()
            )
        });

    // Conflict 3: add_column prepared on v; an append commits first.
    let stale = backend.open(rel).await.unwrap();
    backend
        .open(rel)
        .await
        .unwrap()
        .write(vec![spike_batch(3, 2, 2, 0)])
        .await
        .unwrap();
    let add_col_after_append = stale
        .add_columns()
        .with_fields(vec![deltalake::kernel::StructField::new(
            "extra",
            deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String),
            true,
        )])
        .await
        .map(|t| format!("ok v{}", t.version().unwrap()))
        .unwrap_or_else(|e| {
            format!(
                "error: {}",
                e.to_string().chars().take(100).collect::<String>()
            )
        });

    // A batch without the new nullable column, written to the table that now has it.
    let missing_nullable = backend
        .open(rel)
        .await
        .unwrap()
        .write(vec![spike_batch(3, 9, 2, 0)])
        .await
        .map(|t| format!("ok v{}", t.version().unwrap()))
        .unwrap_or_else(|e| {
            format!(
                "error: {}",
                e.to_string().chars().take(100).collect::<String>()
            )
        });

    // Conflict 4: an append with the old schema is prepared; add_column commits first. Fresh
    // table, so the batch matches the schema the append was prepared against.
    let rel4 = "db/props4/data";
    create_spike_table(backend, rel4, &[]).await.unwrap();
    let stale = backend.open(rel4).await.unwrap();
    let after_add = backend.open(rel4).await.unwrap();
    let _ = after_add
        .add_columns()
        .with_fields(vec![deltalake::kernel::StructField::new(
            "extra2",
            deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String),
            true,
        )])
        .await;
    let append_after_add_col = stale
        .write(vec![spike_batch(3, 3, 2, 0)])
        .await
        .map(|t| format!("ok v{}", t.version().unwrap()))
        .unwrap_or_else(|e| {
            format!(
                "error: {}",
                e.to_string().chars().take(100).collect::<String>()
            )
        });

    println!("RESULT item7 backend={name} create_with_custom_key_default_check: {strict}");
    println!(
        "RESULT item7 backend={name} custom_keys_round_trip={round_trip} keys={:?} set_tbl_properties={set:?} value_after={after:?}",
        props.keys().collect::<Vec<_>>()
    );
    println!(
        "RESULT item7 backend={name} append_prepared_before_property_change: {append_after_meta}"
    );
    println!(
        "RESULT item7 backend={name} property_change_prepared_before_append: {meta_after_append}"
    );
    println!(
        "RESULT item7 backend={name} add_column_prepared_before_append: {add_col_after_append}"
    );
    println!(
        "RESULT item7 backend={name} append_prepared_before_add_column: {append_after_add_col}"
    );
    println!("RESULT item7 backend={name} write_batch_missing_nullable_column: {missing_nullable}");
    assert!(round_trip);
}

async fn item11(name: &str, backend: &Backend) {
    let rel = "db/plain/data";
    create_spike_table(backend, rel, &[]).await.unwrap();
    let c0 = read_commit(backend, rel, 0).await.unwrap();
    let protocol = c0.iter().find_map(|a| a.get("protocol").cloned()).unwrap();
    let rel_dv = "db/dv/data";
    let dv = create_spike_table(backend, rel_dv, &[("delta.enableDeletionVectors", "true")]).await;
    let dv_protocol = match &dv {
        Ok(_) => read_commit(backend, rel_dv, 0)
            .await
            .unwrap()
            .iter()
            .find_map(|a| a.get("protocol").cloned())
            .unwrap(),
        Err(e) => serde_json::json!(format!("create failed: {e}")),
    };
    // Through the typed API: protocol from the snapshot.
    let t = backend.open(rel_dv).await.unwrap();
    let typed = serde_json::to_value(t.snapshot().unwrap().protocol()).unwrap();
    let dv_prop = t
        .snapshot()
        .unwrap()
        .snapshot()
        .table_properties()
        .enable_deletion_vectors;
    // Does delta-rs write deletion vectors on such a table, or rewrite files?
    let t = t.write(vec![spike_batch(4, 0, 4, 0)]).await.unwrap();
    let del = t
        .delete()
        .with_predicate(col("key").eq(lit("w4-s0-r0")))
        .await;
    let del_desc = match &del {
        Ok((t, m)) => {
            let last = read_commit(backend, rel_dv, t.version().unwrap())
                .await
                .unwrap();
            let has_dv = last.iter().any(|a| {
                a.pointer("/add/deletionVector")
                    .map(|d| !d.is_null())
                    .unwrap_or(false)
            });
            format!(
                "ok files_added={} files_removed={} add_has_deletion_vector={has_dv}",
                m.num_added_files, m.num_removed_files
            )
        }
        Err(e) => format!("error: {e}"),
    };
    println!("RESULT item11 backend={name} default_protocol={protocol}");
    println!(
        "RESULT item11 backend={name} dv_enabled_protocol={dv_protocol} typed_snapshot_protocol={typed} enable_deletion_vectors_property={dv_prop:?}"
    );
    println!("RESULT item11 backend={name} delete_on_dv_table: {del_desc}");
}

async fn run(name: &str, backend: Backend) {
    item6(name, &backend).await;
    item7(name, &backend).await;
    item11(name, &backend).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn items_6_7_11_memory() {
    run("memory", Backend::memory()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn items_6_7_11_local() {
    let dir = tempfile::tempdir().unwrap();
    run("local", Backend::Local(dir.path().into())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn items_6_7_11_s3_compatible() {
    let Some(b) = Backend::s3_compatible(&run_prefix("item6")) else {
        println!("RESULT item6 backend=s3 not run (MORAINE_SPIKE_S3_ENDPOINT not set)");
        return;
    };
    run("s3-rustfs", b.clone()).await;
    b.clear().await.unwrap();
}
