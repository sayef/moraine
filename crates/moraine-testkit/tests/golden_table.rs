//! T12 format stability for tables: the checked-in golden database (`golden/table/`, written by
//! `cargo run -p moraine-testkit --bin golden`) holds every kind of object moraine
//! writes into a table: log entries with every action kind, a checkpoint and
//! `_last_checkpoint`, data files, a sidecar, a pin manifest, a lease generation and a
//! queued-write batch. This release must read all of them and answer as recorded in
//! `golden/table.expected.json`, and must reject changed versions with the documented error
//! (docs/FORMAT.md).

use std::sync::Arc;

use moraine::{Database, Error, fuzzing};
use moraine_testkit::golden;
use object_store::memory::InMemory;
use serde_json::Value;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn expected() -> Value {
    let p = golden::dir().join("table.expected.json");
    let b =
        std::fs::read(&p).unwrap_or_else(|e| panic!("{}: {e}; run the golden binary", p.display()));
    serde_json::from_slice(&b).unwrap()
}

/// Every object kind the golden table must hold, so a regeneration that lost one fails here.
#[test]
fn golden_table_holds_every_object_kind() {
    let files = golden::table_files();
    let has = |what: &str, pred: &dyn Fn(&str) -> bool| {
        assert!(
            files.iter().any(|(n, _)| pred(n)),
            "no {what} in golden/table"
        );
    };
    has("log entry", &|n| {
        n.contains("/_delta_log/") && n.ends_with(".json")
    });
    has("checkpoint", &|n| n.ends_with(".checkpoint.parquet"));
    has("_last_checkpoint", &|n| n.ends_with("/_last_checkpoint"));
    has("data file", &|n| {
        !n.contains("/_delta_log/") && n.ends_with(".parquet")
    });
    has("sidecar", &|n| n.ends_with(".osix"));
    has("pin manifest", &|n| n.contains("/meta/pins/"));
    has("lease generation", &|n| n.contains("/meta/leases/"));
    has("queued-write batch", &|n| n.contains("/meta/inbox/"));
}

#[test]
fn golden_table_reads_as_recorded() {
    let files = golden::table_files();
    let got = golden::table_summary(&files);
    let want = expected();
    assert!(
        got == want,
        "the golden table reads differently:\n{got:#}\nrecorded:\n{want:#}"
    );
}

/// Queued writes left in the golden inbox are ingested by this release, and the golden
/// table's maintenance runs on it (verify finds nothing wrong).
#[test]
fn golden_table_ingests_and_verifies() {
    let store = golden::table_store(&golden::table_files());
    runtime().block_on(async {
        let db = Database::builder_for_store(store, golden::TABLE_DB)
            .open()
            .await
            .unwrap();
        let v = db.open_table("v").await.unwrap();
        let before = v.scan().count().await.unwrap();
        let r = v.ingest(false).await.unwrap();
        assert_eq!(r.batches, 1, "{r:?}");
        assert_eq!(v.scan().count().await.unwrap(), before + 8);
        for name in ["t", "v"] {
            let t = db.open_table(name).await.unwrap();
            let report = t.verify().await.unwrap();
            assert!(report.is_ok(), "{name}: {report:?}");
        }
    });
}

/// Replaces the first line of `name`'s log entry 0 that holds `action` with `edit` applied.
fn edit_entry0(
    files: &mut [(String, Vec<u8>)],
    table: &str,
    action: &str,
    edit: impl Fn(&mut Value),
) {
    let path = format!(
        "{}/{table}/data/_delta_log/00000000000000000000.json",
        golden::TABLE_DB
    );
    let (_, body) = files.iter_mut().find(|(n, _)| *n == path).expect("entry 0");
    let text = String::from_utf8(body.clone()).unwrap();
    let mut done = false;
    let lines: Vec<String> = text
        .lines()
        .map(|l| {
            let mut v: Value = serde_json::from_str(l).unwrap();
            if !done && let Some(a) = v.get_mut(action) {
                edit(a);
                done = true;
            }
            v.to_string()
        })
        .collect();
    assert!(done, "no {action} action in entry 0");
    *body = (lines.join("\n") + "\n").into_bytes();
}

fn open_v(files: Vec<(String, Vec<u8>)>) -> moraine::Result<moraine::Table> {
    let store: Arc<InMemory> = golden::table_store(&files);
    runtime().block_on(async {
        let db = Database::builder_for_store(store, golden::TABLE_DB)
            .open()
            .await?;
        db.open_table("v").await
    })
}

/// A newer moraine table format (`moraine.formatVersion`) is refused with
/// `Error::UnsupportedTableFeature`, as docs/FORMAT.md documents.
#[test]
fn newer_table_format_is_unsupported() {
    let mut files = golden::table_files();
    edit_entry0(&mut files, "v", "metaData", |m| {
        m["configuration"]["moraine.formatVersion"] = "2".into();
    });
    assert!(matches!(
        open_v(files),
        Err(Error::UnsupportedTableFeature(_))
    ));
}

/// A Delta protocol beyond what moraine reads (reader version 2, or writer features other than
/// `appendOnly`) is refused with `Error::UnsupportedTableFeature`.
#[test]
fn newer_delta_protocol_is_unsupported() {
    for (reader, writer) in [(2, 2), (3, 7)] {
        let mut files = golden::table_files();
        edit_entry0(&mut files, "v", "protocol", |p| {
            p["minReaderVersion"] = reader.into();
            p["minWriterVersion"] = writer.into();
            if reader == 3 {
                p["readerFeatures"] = serde_json::json!(["deletionVectors"]);
                p["writerFeatures"] = serde_json::json!(["deletionVectors"]);
            }
        });
        assert!(
            matches!(open_v(files), Err(Error::UnsupportedTableFeature(_))),
            "reader {reader} writer {writer}"
        );
    }
}

/// Lease records and pin manifests that do not parse are `Error::Corrupt`, never a panic or
/// a silently empty answer.
#[test]
fn corrupt_lease_and_manifest_are_corrupt() {
    let files = golden::table_files();
    let (lease, _) = files
        .iter()
        .find(|(n, _)| n.contains("/meta/leases/"))
        .unwrap();
    assert!(matches!(
        fuzzing::lease(lease, br#"{"holder":"x"}"#),
        Err(Error::Corrupt { .. })
    ));
    assert!(matches!(
        fuzzing::pin_manifest(br#"{"add":{"path":"x"}}"#),
        Err(Error::Corrupt { .. })
    ));
}
