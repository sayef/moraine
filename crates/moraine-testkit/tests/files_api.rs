//! The file-level API (D67, D68): scans of chosen files, rows by (file, offset), the table id,
//! streamed scans, file replaces from a stream, and compaction keeping caller tags.

use std::sync::Arc;

use arrow::array::RecordBatch;
use futures::TryStreamExt;
use moraine::expr::{col, lit};
use moraine::{CompactionPolicy, Consistency, Database, Error, Replace, Table, TableOptions};
use moraine_testkit::maintcheck::{Row, batch, to_rows};
use moraine_testkit::{SimRng, datagen};
use object_store::memory::InMemory;

async fn table(small_table_bytes: u64) -> (Database, Table) {
    let db = Database::builder_for_store(Arc::new(InMemory::new()), "db")
        .consistency(Consistency::Strong)
        .small_table_bytes(small_table_bytes)
        .open()
        .await
        .unwrap();
    let opts = TableOptions::default().cluster_by(["g"]);
    let t = db
        .create_table("t", datagen::row_schema(), opts)
        .await
        .unwrap();
    let mut rng = SimRng::new(3);
    for n in 0..4 {
        t.append(vec![datagen::rows(&mut rng, &format!("w{n}"), 20, None, 3)])
            .await
            .unwrap();
    }
    (db, t)
}

fn unsorted(b: &[RecordBatch]) -> Vec<Row> {
    let mut out = Vec::new();
    for x in b {
        for i in 0..x.num_rows() {
            out.extend(to_rows(&[x.slice(i, 1)]));
        }
    }
    out
}

#[tokio::test]
async fn a_scan_of_chosen_files_reads_them_in_order() {
    let (_db, t) = table(0).await;
    let snap = t.snapshot().await.unwrap();
    let files: Vec<String> = snap.files().into_iter().map(|f| f.path).collect();
    let chosen = vec![files[2].clone(), files[0].clone()];
    let got = unsorted(&snap.scan().files(chosen.clone()).collect().await.unwrap());
    let mut want = unsorted(
        &snap
            .scan()
            .files([files[2].clone()])
            .collect()
            .await
            .unwrap(),
    );
    want.extend(unsorted(
        &snap
            .scan()
            .files([files[0].clone()])
            .collect()
            .await
            .unwrap(),
    ));
    assert_eq!(got, want);
    assert_eq!(got.len(), 40);
    assert!(got[..20].iter().all(|r| r.op == "w2") && got[20..].iter().all(|r| r.op == "w0"));
    // Filters, projections and limits apply as usual.
    let n = snap
        .scan()
        .files(chosen.clone())
        .filter(col("op").eq(lit("w0")))
        .select(["k"])
        .count()
        .await
        .unwrap();
    assert_eq!(n, 20);
    assert_eq!(
        snap.scan().files(chosen).limit(25).count().await.unwrap(),
        25
    );
    let e = snap
        .scan()
        .files(["nope.parquet"])
        .collect()
        .await
        .unwrap_err();
    assert!(
        matches!(e, Error::FilesChanged(ref p) if p == "nope.parquet"),
        "{e:?}"
    );
}

#[tokio::test]
async fn rows_by_offset_match_the_scan_of_their_file() {
    let (_db, t) = table(0).await;
    let snap = t.snapshot().await.unwrap();
    let f = snap.files()[1].clone();
    let rows = unsorted(&snap.scan().files([f.path.clone()]).collect().await.unwrap());
    let offsets = [7u64, 0, 19, 7];
    let got = snap.take(&f.path, &offsets).collect().await.unwrap();
    assert_eq!(got.num_rows(), 4);
    let want: Vec<Row> = offsets.iter().map(|o| rows[*o as usize].clone()).collect();
    assert_eq!(unsorted(&[got]), want);
    let ks = snap
        .take(&f.path, &[3])
        .select(["k"])
        .collect()
        .await
        .unwrap();
    assert_eq!(ks.num_columns(), 1);
    assert_eq!(ks.num_rows(), 1);
    assert!(matches!(
        snap.take(&f.path, &[20]).collect().await,
        Err(Error::InvalidQuery(_))
    ));
    assert!(matches!(
        snap.take("nope.parquet", &[0]).collect().await,
        Err(Error::FilesChanged(_))
    ));
    assert_eq!(
        snap.take(&f.path, &[]).collect().await.unwrap().num_rows(),
        0
    );
}

#[tokio::test]
async fn the_table_id_is_stable_and_new_for_a_new_table() {
    let store = Arc::new(InMemory::new());
    let open = |store: Arc<InMemory>| async move {
        Database::builder_for_store(store, "db")
            .open()
            .await
            .unwrap()
    };
    let db = open(store.clone()).await;
    let t = db
        .create_table("t", datagen::row_schema(), TableOptions::default())
        .await
        .unwrap();
    let id = t.id();
    assert!(!id.is_empty());
    let mut rng = SimRng::new(1);
    for n in 0..3 {
        t.append(vec![datagen::rows(&mut rng, &format!("w{n}"), 2, None, 2)])
            .await
            .unwrap();
    }
    t.add_column(arrow::datatypes::Field::new(
        "x",
        arrow::datatypes::DataType::Int64,
        true,
    ))
    .await
    .unwrap();
    t.checkpoint().await.unwrap();
    let again = open(store.clone()).await.open_table("t").await.unwrap();
    assert_eq!(again.id(), id);
    assert_eq!(again.snapshot().await.unwrap().table_id(), id);
    let other = db
        .create_table("u", datagen::row_schema(), TableOptions::default())
        .await
        .unwrap();
    assert_ne!(other.id(), id);
}

#[tokio::test]
async fn a_streamed_scan_returns_what_collect_does() {
    for small in [0, 1 << 30] {
        let (_db, t) = table(small).await;
        let all = t.scan().collect().await.unwrap();
        let streamed: Vec<RecordBatch> = t.scan().stream().try_collect().await.unwrap();
        assert_eq!(unsorted(&streamed), unsorted(&all));
        let some: Vec<RecordBatch> = t
            .scan()
            .filter(col("op").eq(lit("w1")))
            .limit(7)
            .stream()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(unsorted(&some).len(), 7);
        assert!(unsorted(&some).iter().all(|r| r.op == "w1"));
        let none: Vec<RecordBatch> = t.scan().limit(0).stream().try_collect().await.unwrap();
        assert!(none.is_empty());
    }
}

/// A replace from a stream writes its rows a target file size at a time, with the replace's
/// tags; an error from the stream fails it and deletes what was written for it.
#[tokio::test]
async fn a_replace_from_a_stream_writes_files_as_it_reads() {
    let (_db, t) = table(0).await;
    let def = t.definition();
    assert!(def.target_file_bytes > 0);
    let snap = t.snapshot().await.unwrap();
    let old: Vec<String> = snap.files().into_iter().map(|f| f.path).collect();
    // Rewrites every file without the rows of `w3`, file by file.
    let kept = snap
        .scan()
        .files(old.clone())
        .filter(col("op").not_eq(lit("w3")))
        .stream();
    t.replace_files(
        Replace::new()
            .remove_all(old.clone())
            .add_stream(kept)
            .tag("kind", "kept"),
    )
    .await
    .unwrap();
    let after = t.snapshot().await.unwrap();
    assert!(
        after
            .files()
            .iter()
            .all(|f| f.tags.get("kind").map(String::as_str) == Some("kept"))
    );
    let rows = to_rows(&after.scan().collect().await.unwrap());
    assert_eq!(rows.len(), 60);
    assert!(rows.iter().all(|r| r.op != "w3"));

    // A table whose every batch makes a file of its own, so the failure comes after an upload.
    let store = Arc::new(InMemory::new());
    let db = Database::builder_for_store(store.clone(), "db")
        .open()
        .await
        .unwrap();
    let opts = TableOptions::default().target_file_bytes(1);
    let t = db
        .create_table("small", datagen::row_schema(), opts)
        .await
        .unwrap();
    let parquet = || async {
        use object_store::ObjectStore;
        let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
        listed
            .iter()
            .filter(|m| m.location.as_ref().ends_with(".parquet"))
            .filter(|m| !m.location.as_ref().contains("_delta_log"))
            .count()
    };
    let before = t.snapshot().await.unwrap().files();
    let one = |k: &str| {
        batch(&[Row {
            k: k.into(),
            op: "s".into(),
            j: 0,
            g: None,
        }])
    };
    let failing = futures::stream::iter(vec![
        Ok(one("a")),
        Ok(one("b")),
        Err(Error::InvalidQuery("the source failed".into())),
    ]);
    let e = t
        .replace_files(Replace::new().add_stream(failing))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::InvalidQuery(_)), "{e:?}");
    assert_eq!(t.snapshot().await.unwrap().files(), before);
    assert_eq!(parquet().await, 0, "the files written for it are left");
}

/// Compaction merges only files with the same caller tags, and the merged files keep them.
#[tokio::test]
async fn compaction_keeps_caller_tags() {
    let (_db, t) = table(0).await;
    let snap = t.snapshot().await.unwrap();
    let files: Vec<String> = snap.files().into_iter().map(|f| f.path).collect();
    // Two tagged files at the end, two untagged at the start.
    let tagged = snap
        .scan()
        .files(files[2..].to_vec())
        .collect()
        .await
        .unwrap();
    t.replace_files(
        Replace::new()
            .remove_all(files[2..].to_vec())
            .add_rows(tagged)
            .tag("kind", "stats"),
    )
    .await
    .unwrap();
    let mut rng = SimRng::new(9);
    t.replace_files(
        Replace::new()
            .add_rows(vec![datagen::rows(&mut rng, "more", 5, None, 2)])
            .tag("kind", "stats"),
    )
    .await
    .unwrap();
    let before = t.snapshot().await.unwrap();
    let rows_before = to_rows(&before.scan().collect().await.unwrap());
    let report = t.compact(&CompactionPolicy::full()).await.unwrap();
    assert!(report.steps >= 1, "{report:?}");
    let after = t.snapshot().await.unwrap();
    let kinds: Vec<Option<String>> = after
        .files()
        .iter()
        .map(|f| f.tags.get("kind").cloned())
        .collect();
    assert_eq!(
        kinds,
        vec![None, Some("stats".into())],
        "{:?}",
        after.files()
    );
    assert_eq!(to_rows(&after.scan().collect().await.unwrap()), rows_before);
}
