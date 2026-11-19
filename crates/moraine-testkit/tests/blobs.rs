//! Blobs: the blob type round trip, pass-through copies, staged uploads, range
//! and streaming reads, collection, `verify`, and the request budget of every path.
//!
//! Cases B1 to B11 and scenario 8 (`blob_reference_reuse`); scenario 7 and the simulations are
//! in `blobs_sim.rs`, the crash failpoints in `crash_blobs.rs`.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use moraine::blob::{self, BlobColumn, BlobRef, BlobReuse};
use moraine::expr::{col, lit};
use moraine::store::{MetricsSnapshot, Op};
use moraine::{
    CompactionPolicy, Consistency, Database, Error, MaintainOptions, Table, TableOptions,
};
use moraine_testkit::{SimClock, SimStore};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("n", DataType::Int64, true),
        blob::field("body", true),
    ]))
}

fn options() -> TableOptions {
    TableOptions::default()
        .primary_key(["id"])
        .grace_period(Duration::from_secs(600))
}

/// Deterministic bytes of length `n` for value `seed`.
fn payload(seed: u64, n: usize) -> Bytes {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect::<Vec<u8>>()
        .into()
}

enum V {
    Bytes(Bytes),
    Ref(BlobRef),
    Null,
}

fn batch(rows: Vec<(i64, V)>) -> RecordBatch {
    let ids = Int64Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>());
    let ns = Int64Array::from(rows.iter().map(|r| Some(r.0 * 10)).collect::<Vec<_>>());
    let mut c = BlobColumn::new();
    for (_, v) in &rows {
        match v {
            V::Bytes(b) => c.push_bytes(b, Some("application/octet-stream")),
            V::Ref(r) => c.push_ref(r),
            V::Null => c.push_null(),
        }
    }
    RecordBatch::try_new(
        schema(),
        vec![Arc::new(ids) as ArrayRef, Arc::new(ns), c.finish()],
    )
    .unwrap()
}

/// The blob of row `id`, as its row holds it.
async fn row_ref(t: &Table, id: i64) -> Option<BlobRef> {
    let rows = t
        .scan()
        .filter(col("id").eq(lit(id)))
        .collect()
        .await
        .unwrap();
    let b = rows.iter().find(|b| b.num_rows() > 0)?;
    blob::refs(b, "body").unwrap().remove(0)
}

fn counts(m: &MetricsSnapshot) -> Vec<(&'static str, u64)> {
    Op::all()
        .iter()
        .filter(|o| m.count(**o) > 0)
        .map(|o| (o.name(), m.count(*o)))
        .collect()
}

async fn mem_db(store: Arc<dyn ObjectStore>) -> Database {
    Database::builder_for_store(store, "db")
        .consistency(Consistency::Strong)
        .open()
        .await
        .unwrap()
}

async fn blob_objects(store: &InMemory) -> Vec<String> {
    let prefix = Path::from("db/t/blobs");
    let mut v: Vec<String> = store
        .list(Some(&prefix))
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await
        .unwrap();
    v.sort();
    v
}

/// Inline and external values, empty and null ones (B5), a value above one upload part (B6),
/// ranges and streams: what was written reads back, and the threshold decides where it lives.
#[tokio::test]
async fn round_trip_inline_external_and_multipart_b5_b6() {
    let store = Arc::new(InMemory::new());
    let db = mem_db(store.clone()).await;
    let t = db.create_table("t", schema(), options()).await.unwrap();
    let small = payload(1, 1000);
    let at_threshold = payload(2, 256 << 10);
    let large = payload(3, (256 << 10) + 1);
    let huge = payload(4, (16 << 20) * 2 + 12_345);
    t.upsert(vec![batch(vec![
        (1, V::Bytes(small.clone())),
        (2, V::Bytes(at_threshold.clone())),
        (3, V::Bytes(large.clone())),
        (4, V::Bytes(huge.clone())),
        (5, V::Bytes(Bytes::new())),
        (6, V::Null),
    ])])
    .await
    .unwrap();
    let objects = blob_objects(&store).await;
    assert_eq!(objects.len(), 2, "{objects:?}");
    for (id, want) in [(1, &small), (2, &at_threshold), (3, &large), (4, &huge)] {
        let r = row_ref(&t, id).await.unwrap();
        assert_eq!(r.is_inline(), want.len() <= 256 << 10, "row {id}");
        assert_eq!(r.size(), want.len() as u64);
        assert_eq!(r.content_type(), Some("application/octet-stream"));
        let b = t.blob(&r).unwrap();
        assert_eq!(&b.bytes().await.unwrap(), want, "row {id}");
        let mid = want.len() as u64 / 2;
        let end = (mid + 100).min(want.len() as u64);
        assert_eq!(
            b.range(mid..end).await.unwrap(),
            want.slice(mid as usize..end as usize)
        );
        let streamed: Vec<Bytes> = b.stream().await.unwrap().try_collect().await.unwrap();
        assert_eq!(streamed.concat(), want.to_vec(), "row {id}");
        assert!(matches!(
            b.range(0..want.len() as u64 + 1).await,
            Err(Error::InvalidQuery(_))
        ));
    }
    // Zero length is stored inline with size 0; null is null (case B5).
    let empty = row_ref(&t, 5).await.unwrap();
    assert!(empty.is_inline() && empty.size() == 0);
    assert!(t.blob(&empty).unwrap().bytes().await.unwrap().is_empty());
    assert!(row_ref(&t, 6).await.is_none());
}

/// Every blob path costs exactly the requests it must (D18 for blobs): an external value is
/// one PUT beside the data PUT and the log PUT; a value above 16 MiB is a multipart upload
/// (initiation, one PUT per 16 MiB part, completion); a pass-through reference is one server
/// copy; a read is one GET, a range one ranged GET, a stream one ranged GET per 4 MiB; a
/// staged upload committed by its own handle costs no copy.
#[tokio::test]
async fn request_budgets() {
    let store = Arc::new(InMemory::new());
    let db = mem_db(store.clone()).await;
    let t = db.create_table("t", schema(), options()).await.unwrap();
    t.set_consistency(Consistency::Manual);
    t.upsert(vec![batch(vec![(100, V::Null)])]).await.unwrap();
    let m = || db.store_metrics().snapshot();

    // Inline only: as before blobs, one data PUT and one log PUT (the upsert reads nothing:
    // the key is new and the only file's statistics exclude it).
    let before = m();
    t.upsert(vec![batch(vec![(1, V::Bytes(payload(1, 10)))])])
        .await
        .unwrap();
    let inline = counts(&m().since(&before));

    // One external value: one more PUT.
    let before = m();
    t.upsert(vec![batch(vec![(2, V::Bytes(payload(2, 300_000)))])])
        .await
        .unwrap();
    let external = counts(&m().since(&before));
    let plus_one = |base: &[(&'static str, u64)]| -> Vec<(&'static str, u64)> {
        base.iter()
            .map(|(n, c)| (*n, if *n == "put" { c + 1 } else { *c }))
            .collect()
    };
    assert_eq!(external, plus_one(&inline), "inline {inline:?}");

    // Above 16 MiB: initiation, three parts, completion.
    let before = m();
    t.upsert(vec![batch(vec![(3, V::Bytes(payload(3, (32 << 20) + 1)))])])
        .await
        .unwrap();
    let d = m().since(&before);
    assert_eq!(
        (
            d.count(Op::Multipart),
            d.count(Op::Part),
            d.count(Op::Complete)
        ),
        (1, 3, 1),
        "{d}"
    );

    // A pass-through reference: one copy, no bytes through the client.
    let r = row_ref(&t, 2).await.unwrap();
    let before = m();
    t.upsert(vec![batch(vec![(4, V::Ref(r.clone()))])])
        .await
        .unwrap();
    let d = m().since(&before);
    assert_eq!(d.count(Op::Copy), 1, "{d}");
    assert!(d.bytes_written < 100_000, "{d}");
    let copied = row_ref(&t, 4).await.unwrap();
    assert_ne!(copied.key(), r.key());
    assert_eq!((copied.size(), copied.crc32c()), (r.size(), r.crc32c()));

    // Reads: one GET for the whole value, one ranged GET for a range.
    let b = t.blob(&copied).unwrap();
    let before = m();
    b.bytes().await.unwrap();
    assert_eq!(counts(&m().since(&before)), vec![("get", 1)]);
    let before = m();
    b.range(10..20).await.unwrap();
    assert_eq!(counts(&m().since(&before)), vec![("get_range", 1)]);
    let before = m();
    let _: Vec<Bytes> = b.stream().await.unwrap().try_collect().await.unwrap();
    assert_eq!(counts(&m().since(&before)), vec![("get_range", 1)]);
    // A large value streams as ranged GETs of 4 MiB, the first of which checks the size (D82).
    let big = t.blob(&row_ref(&t, 3).await.unwrap()).unwrap();
    let before = m();
    let s: Vec<Bytes> = big.stream().await.unwrap().try_collect().await.unwrap();
    assert_eq!(s.iter().map(Bytes::len).sum::<usize>(), (32 << 20) + 1);
    assert_eq!(counts(&m().since(&before)), vec![("get_range", 9)]);

    // A staged upload (one PUT), then its commit through the same handle: no copy.
    let before = m();
    let staged = t
        .upload_blob(
            futures::stream::iter([Ok::<_, std::io::Error>(payload(5, 400_000))]),
            None,
        )
        .await
        .unwrap();
    assert_eq!(counts(&m().since(&before)), vec![("put", 1)]);
    let before = m();
    t.upsert(vec![batch(vec![(5, V::Ref(staged.clone()))])])
        .await
        .unwrap();
    let d = m().since(&before);
    assert_eq!(d.count(Op::Copy), 0, "{d}");
    assert_eq!(row_ref(&t, 5).await.unwrap().key(), staged.key());
}

/// Case B3 and scenario 8 (`blob_reference_reuse`): a row is deleted, its old reference is
/// inserted again, collection runs past the grace period; the new row's blob stays readable,
/// because the write copied it to a key of its own. Re-inserting a reference whose object was
/// already collected fails with `BlobNotFound` and writes nothing.
#[tokio::test]
async fn blob_reference_reuse() {
    let clock = SimClock::new();
    let store = Arc::new(SimStore::new(clock.clone()));
    let db = Database::builder_for_store(store.clone(), "db")
        .clock(Arc::new(moraine_testkit::ClientClock::new(
            clock.clone(),
            None,
            0,
            1,
        )))
        .consistency(Consistency::Strong)
        .open()
        .await
        .unwrap();
    let t = db.create_table("t", schema(), options()).await.unwrap();
    let value = payload(9, 500_000);
    t.upsert(vec![batch(vec![(1, V::Bytes(value.clone()))])])
        .await
        .unwrap();
    let old = row_ref(&t, 1).await.unwrap();
    t.delete_keys(&[vec![moraine::expr::ScalarValue::Int64(Some(1))]])
        .await
        .unwrap();
    clock.advance(Duration::from_secs(60));
    // The caller kept the reference and inserts it again under another key.
    t.upsert(vec![batch(vec![(2, V::Ref(old.clone()))])])
        .await
        .unwrap();
    let new = row_ref(&t, 2).await.unwrap();
    assert_ne!(new.key(), old.key());
    let quiet = MaintainOptions::default().leases(false).compaction(None);
    let mut deleted = 0;
    for _ in 0..3 {
        clock.advance(Duration::from_secs(400));
        deleted += t
            .maintain(&quiet)
            .await
            .unwrap()
            .vacuum
            .unwrap()
            .blobs_deleted;
    }
    // The old object went once nothing referenced it for the grace period (case B9).
    assert_eq!(deleted, 1);
    assert_eq!(&t.blob(&new).unwrap().bytes().await.unwrap(), &value);
    assert!(matches!(
        t.blob(&old).unwrap().bytes().await,
        Err(Error::BlobNotFound(_))
    ));
    // Too late now: the old object is gone, and the write fails whole.
    let v = t.version();
    assert!(matches!(
        t.upsert(vec![batch(vec![(3, V::Ref(old))])]).await,
        Err(Error::BlobNotFound(_))
    ));
    assert_eq!(t.refresh().await.unwrap(), v);
    assert!(t.verify().await.unwrap().is_ok());
}

/// `BlobReuse::TrustCaller` commits a reference as it is.
#[tokio::test]
async fn trust_caller_skips_the_copy() {
    let store = Arc::new(InMemory::new());
    let db = Database::builder_for_store(store.clone(), "db")
        .blob_reuse(BlobReuse::TrustCaller)
        .open()
        .await
        .unwrap();
    let t = db.create_table("t", schema(), options()).await.unwrap();
    t.upsert(vec![batch(vec![(1, V::Bytes(payload(1, 300_000)))])])
        .await
        .unwrap();
    let r = row_ref(&t, 1).await.unwrap();
    t.upsert(vec![batch(vec![(2, V::Ref(r.clone()))])])
        .await
        .unwrap();
    assert_eq!(row_ref(&t, 2).await.unwrap().key(), r.key());
    assert_eq!(blob_objects(&store).await.len(), 1);
}

/// Case B4: a missing object fails that value only; the row reads, and `verify` names it.
/// Case B10: changed bytes are an error, never wrong bytes, for whole reads and streams.
#[tokio::test]
async fn missing_and_corrupt_objects_b4_b10() {
    let store = Arc::new(InMemory::new());
    let db = mem_db(store.clone()).await;
    let t = db.create_table("t", schema(), options()).await.unwrap();
    t.upsert(vec![batch(vec![
        (1, V::Bytes(payload(1, 300_000))),
        (2, V::Bytes(payload(2, 300_000))),
    ])])
    .await
    .unwrap();
    let (r1, r2) = (row_ref(&t, 1).await.unwrap(), row_ref(&t, 2).await.unwrap());
    let loc = |r: &BlobRef| {
        let k = r.key().unwrap();
        Path::from(format!("db/t/blobs/{k}"))
    };
    store.delete(&loc(&r1)).await.unwrap();
    let mut bad = payload(2, 300_000).to_vec();
    bad[1000] ^= 1;
    store.put(&loc(&r2), PutPayload::from(bad)).await.unwrap();
    // The rows still read.
    assert_eq!(t.scan().count().await.unwrap(), 2);
    assert!(matches!(
        t.blob(&r1).unwrap().bytes().await,
        Err(Error::BlobNotFound(_))
    ));
    assert!(matches!(
        t.blob(&r2).unwrap().bytes().await,
        Err(Error::BlobCorrupt(_))
    ));
    let streamed: Result<Vec<Bytes>, Error> = t
        .blob(&r2)
        .unwrap()
        .stream()
        .await
        .unwrap()
        .try_collect()
        .await;
    assert!(matches!(streamed, Err(Error::BlobCorrupt(_))));
    let v = t.verify().await.unwrap();
    assert_eq!(v.missing_blobs, vec![r1.key().unwrap().to_string()]);
    assert!(!v.is_ok());
}

/// Case B8: compaction and an update of another column carry references unchanged, with no
/// copy; an update cannot assign a blob column.
#[tokio::test]
async fn rewrites_carry_references_b8() {
    let store = Arc::new(InMemory::new());
    let db = mem_db(store.clone()).await;
    let t = db.create_table("t", schema(), options()).await.unwrap();
    for id in 0..3 {
        t.upsert(vec![batch(vec![(
            id,
            V::Bytes(payload(id as u64, 300_000)),
        )])])
        .await
        .unwrap();
    }
    let before: Vec<BlobRef> = futures::future::join_all((0..3).map(|i| row_ref(&t, i)))
        .await
        .into_iter()
        .map(Option::unwrap)
        .collect();
    let copies = db.store_metrics().snapshot().count(Op::Copy);
    t.update(col("id").eq(lit(1i64)), vec![("n".into(), lit(7i64))])
        .await
        .unwrap();
    let r = t
        .maintain(
            &MaintainOptions::default()
                .leases(false)
                .compaction(Some(CompactionPolicy::full())),
        )
        .await
        .unwrap();
    assert!(r.compaction.unwrap().steps > 0);
    assert_eq!(t.snapshot_in_hand().files().len(), 1);
    for (i, b) in before.iter().enumerate() {
        assert_eq!(row_ref(&t, i as i64).await.as_ref(), Some(b));
    }
    assert_eq!(db.store_metrics().snapshot().count(Op::Copy), copies);
    assert!(matches!(
        t.update(
            lit(true),
            vec![("body".into(), lit(moraine::expr::ScalarValue::Null))]
        )
        .await,
        Err(Error::InvalidQuery(_))
    ));
}

/// Cases B1 and B9: an upload whose write never commits, and the object of a deleted row, are
/// collected once older than the grace period; live ones and those of a pinned version stay.
#[tokio::test]
async fn collection_b1_b9_and_pins() {
    let clock = SimClock::new();
    let store = Arc::new(SimStore::new(clock.clone()));
    let db = Database::builder_for_store(store.clone(), "db")
        .clock(Arc::new(moraine_testkit::ClientClock::new(
            clock.clone(),
            None,
            0,
            1,
        )))
        .consistency(Consistency::Strong)
        .open()
        .await
        .unwrap();
    let t = db.create_table("t", schema(), options()).await.unwrap();
    t.upsert(vec![batch(vec![
        (1, V::Bytes(payload(1, 300_000))),
        (2, V::Bytes(payload(2, 300_000))),
        (3, V::Bytes(payload(3, 300_000))),
    ])])
    .await
    .unwrap();
    let keep = row_ref(&t, 1).await.unwrap();
    let pinned = row_ref(&t, 2).await.unwrap();
    let gone = row_ref(&t, 3).await.unwrap();
    // A staged upload that no write commits (case B1).
    let orphan = t
        .upload_blob(
            futures::stream::iter([Ok::<_, std::io::Error>(payload(4, 1000))]),
            None,
        )
        .await
        .unwrap();
    t.pin("before", t.version()).await.unwrap();
    t.delete_keys(&[
        vec![moraine::expr::ScalarValue::Int64(Some(2))],
        vec![moraine::expr::ScalarValue::Int64(Some(3))],
    ])
    .await
    .unwrap();
    let quiet = MaintainOptions::default().leases(false).compaction(None);
    // Within the grace period nothing goes.
    let r = t.maintain(&quiet).await.unwrap().vacuum.unwrap();
    assert_eq!(r.blobs_deleted, 0);
    clock.advance(Duration::from_secs(700));
    let r = t.maintain(&quiet).await.unwrap().vacuum.unwrap();
    // The orphan; the pin keeps the deleted rows' objects.
    assert_eq!(r.blobs_deleted, 1, "{r:?}");
    for (r, alive) in [
        (&keep, true),
        (&pinned, true),
        (&gone, true),
        (&orphan, false),
    ] {
        let got = t.blob(r).unwrap().bytes().await;
        assert_eq!(got.is_ok(), alive, "{:?}: {got:?}", r.key());
    }
    // Unpinned, the deleted rows' objects go once the unpin is older than the grace period,
    // as the files only the pin kept do.
    t.unpin("before").await.unwrap();
    let mut deleted = 0;
    for _ in 0..2 {
        clock.advance(Duration::from_secs(700));
        deleted += t
            .maintain(&quiet)
            .await
            .unwrap()
            .vacuum
            .unwrap()
            .blobs_deleted;
    }
    assert_eq!(deleted, 2);
    assert!(t.blob(&keep).unwrap().bytes().await.is_ok());
    assert!(t.blob(&gone).unwrap().bytes().await.is_err());
    assert!(t.verify().await.unwrap().is_ok());
}

/// A table without blob columns makes no blob request in vacuum.
#[tokio::test]
async fn vacuum_without_blob_columns_lists_no_blobs() {
    let store = Arc::new(InMemory::new());
    let db = mem_db(store.clone()).await;
    let s = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let t = db
        .create_table("plain", s.clone(), TableOptions::default())
        .await
        .unwrap();
    let b = RecordBatch::try_new(s, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    t.append(vec![b]).await.unwrap();
    let before = db.store_metrics().snapshot();
    t.vacuum().await.unwrap();
    let d = db.store_metrics().snapshot().since(&before);
    // `data/`, `idx/`, `meta/pins/` and `meta/clock/`, as before blobs: no `blobs/` listing.
    assert_eq!(d.listings(), 4, "{d}");
}

/// The local filesystem's copy is a new object with a fresh modification time, as on the
/// clouds; its own hard link would share the source's.
#[tokio::test]
async fn local_copy_has_a_fresh_time() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::builder(dir.path().to_str().unwrap())
        .open()
        .await
        .unwrap();
    let t = db.create_table("t", schema(), options()).await.unwrap();
    t.upsert(vec![batch(vec![(1, V::Bytes(payload(1, 300_000)))])])
        .await
        .unwrap();
    let r = row_ref(&t, 1).await.unwrap();
    let path = |r: &BlobRef| dir.path().join("t/blobs").join(r.key().unwrap());
    let old = std::fs::File::options().write(true).open(path(&r)).unwrap();
    let past = std::time::SystemTime::now() - Duration::from_secs(7200);
    old.set_modified(past).unwrap();
    t.upsert(vec![batch(vec![(2, V::Ref(r.clone()))])])
        .await
        .unwrap();
    let copy = row_ref(&t, 2).await.unwrap();
    let m = std::fs::metadata(path(&copy)).unwrap().modified().unwrap();
    assert!(m > past + Duration::from_secs(3600));
    assert_eq!(
        t.blob(&copy).unwrap().bytes().await.unwrap(),
        payload(1, 300_000)
    );
}

/// Queued writes take blob bytes; a reference is refused (the ingestor would copy it later).
#[tokio::test]
async fn queued_writes_refuse_references() {
    let store = Arc::new(InMemory::new());
    let db = mem_db(store.clone()).await;
    let t = db
        .create_table(
            "t",
            schema(),
            TableOptions::default().grace_period(Duration::from_secs(600)),
        )
        .await
        .unwrap();
    t.append(vec![batch(vec![(1, V::Bytes(payload(1, 300_000)))])])
        .await
        .unwrap();
    let r = row_ref(&t, 1).await.unwrap();
    assert!(matches!(
        t.submit(vec![batch(vec![(2, V::Ref(r))])]).await,
        Err(Error::InvalidBatch(_))
    ));
    t.submit(vec![batch(vec![(3, V::Bytes(payload(3, 300_000)))])])
        .await
        .unwrap();
    t.ingest(false).await.unwrap();
    let r3 = row_ref(&t, 3).await.unwrap();
    assert!(!r3.is_inline());
    assert_eq!(
        t.blob(&r3).unwrap().bytes().await.unwrap(),
        payload(3, 300_000)
    );
}

/// Invalid values are refused before anything is written.
#[tokio::test]
async fn invalid_values_are_refused() {
    let store = Arc::new(InMemory::new());
    let db = mem_db(store.clone()).await;
    let t = db.create_table("t", schema(), options()).await.unwrap();
    let traversal = BlobRef::external("../../etc/passwd", 4, 0, None);
    assert!(matches!(
        t.upsert(vec![batch(vec![(1, V::Ref(traversal))])]).await,
        Err(Error::InvalidBatch(_))
    ));
    // Both fields set: built by hand.
    let b = batch(vec![(1, V::Bytes(payload(1, 10)))]);
    let s = b.column(2).as_struct();
    let mut cols = s.columns().to_vec();
    cols[1] = Arc::new(arrow::array::StringArray::from(vec![Some(
        "ab/0198c2a0-0000-7000-8000-0000000000ab",
    )]));
    let both = arrow::array::StructArray::try_new(s.fields().clone(), cols, None).unwrap();
    let b = RecordBatch::try_new(
        schema(),
        vec![b.column(0).clone(), b.column(1).clone(), Arc::new(both)],
    )
    .unwrap();
    assert!(matches!(
        t.upsert(vec![b]).await,
        Err(Error::InvalidBatch(_))
    ));
    assert!(blob_objects(&store).await.is_empty());
}
