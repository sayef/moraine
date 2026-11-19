//! Index specs in table properties: creation, drops and replacements, races between clients
//! (D9), malformed properties and kinds a client has not registered (D33).

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use moraine::testing::{INDEXES_PROPERTY, canonical_json, spec_hash};
use moraine::{Database, Error, IndexSpec, TableOptions};
use moraine_testkit::toykind::SumKind;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use serde_json::json;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("n", DataType::Int64, true),
    ]))
}

async fn db(store: &Arc<InMemory>, kind: bool) -> Database {
    let mut b = Database::builder_for_store(store.clone(), "db");
    if kind {
        b = b.index_kind(Arc::new(SumKind));
    }
    b.open().await.unwrap()
}

fn sum(name: &str) -> IndexSpec {
    IndexSpec::new(name, "test-sum", &["n"])
}

#[tokio::test]
async fn create_drop_replace() {
    let store = Arc::new(InMemory::new());
    let t = db(&store, true)
        .await
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    assert!(t.indexes().unwrap().is_empty());
    let created = t.create_index(sum("a")).await.unwrap();
    assert_eq!(created.params, json!({}), "the kind filled its defaults");
    let v = t.version();
    // An equal spec changes nothing; a different one is refused.
    t.create_index(sum("a")).await.unwrap();
    assert_eq!(t.version(), v);
    let other = sum("a").policy(json!({"min_rows": 5}));
    assert!(matches!(t.create_index(other.clone()).await, Err(Error::IndexExists(n)) if n == "a"));
    // Replace swaps it in one commit.
    t.replace_index(other).await.unwrap();
    assert_eq!(t.version(), v + 1);
    assert_eq!(t.indexes().unwrap()[0].policy, json!({"min_rows": 5}));
    t.drop_index("a").await.unwrap();
    assert!(t.indexes().unwrap().is_empty());
    t.drop_index("a").await.unwrap();
    assert_eq!(
        t.version(),
        v + 2,
        "dropping a missing index commits nothing"
    );

    // Bad specs fail before any commit.
    assert!(matches!(
        t.create_index(IndexSpec::new("Bad", "test-sum", &["n"]))
            .await,
        Err(Error::InvalidIndexSpec(_))
    ));
    assert!(matches!(
        t.create_index(IndexSpec::new("b", "test-sum", &["id", "n"]))
            .await,
        Err(Error::InvalidIndexSpec(_))
    ));
    assert!(matches!(
        t.create_index(IndexSpec::new("b", "nope", &["n"])).await,
        Err(Error::IndexKindNotRegistered(k)) if k.name == "nope"
    ));
    // A reopened handle sees the same specs.
    let again = db(&store, true).await.open_table("t").await.unwrap();
    assert_eq!(again.indexes().unwrap(), t.indexes().unwrap());
}

/// The spec hash is pinned: a change to canonicalisation or to the hashed fields shows up
/// here, not as a silent rebuild of every sidecar (V7).
#[test]
fn pinned_spec_hashes() {
    let s = sum("a").params(json!({"z": 1, "a": [1, 2]}));
    let h = spec_hash(&SumKind, &s);
    assert_eq!(h, "659c7b12ef9eee88");
    // Policy, search defaults and the name are not hashed; params and columns are.
    let mut t = s
        .clone()
        .policy(json!({"min_rows": 9}))
        .search(json!({"x": 1}));
    t.name = "other".into();
    assert_eq!(spec_hash(&SumKind, &t), h);
    let u = s.clone().params(json!({"z": 2, "a": [1, 2]}));
    assert_ne!(spec_hash(&SumKind, &u), h);
    // Key order inside params does not matter.
    let v: serde_json::Value = serde_json::from_str(r#"{"a":[1,2],"z":1}"#).unwrap();
    assert_eq!(spec_hash(&SumKind, &sum("a").params(v)), h);
    assert_eq!(canonical_json(&json!({"b": 1, "a": 2})), r#"{"a":2,"b":1}"#);
}

/// Two clients change indexes at once: both changes land (D9), an equal create is idempotent,
/// a conflicting create fails with `IndexExists`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_changes_race() {
    let store = Arc::new(InMemory::new());
    let a = db(&store, true)
        .await
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    let b = db(&store, true).await.open_table("t").await.unwrap();
    for round in 0..10 {
        let (x, y) = (format!("x{round}"), format!("y{round}"));
        let (ra, rb) = tokio::join!(a.create_index(sum(&x)), b.create_index(sum(&y)));
        ra.unwrap();
        rb.unwrap();
        a.refresh().await.unwrap();
        let names: Vec<String> = a.indexes().unwrap().into_iter().map(|s| s.name).collect();
        assert!(names.contains(&x) && names.contains(&y), "{names:?}");
        // Same name, same spec: both succeed. Same name, different specs: one fails.
        let z = format!("z{round}");
        let (ra, rb) = tokio::join!(a.create_index(sum(&z)), b.create_index(sum(&z)));
        ra.unwrap();
        rb.unwrap();
        let w = format!("w{round}");
        let other = sum(&w).policy(json!({"min_rows": 2}));
        let (ra, rb) = tokio::join!(a.create_index(sum(&w)), b.create_index(other));
        assert!(
            ra.is_ok() != rb.is_ok(),
            "exactly one of two conflicting creates wins: {ra:?} {rb:?}"
        );
        let (ra, rb) = tokio::join!(a.drop_index(&x), b.drop_index(&x));
        ra.unwrap();
        rb.unwrap();
    }
    a.refresh().await.unwrap();
    b.refresh().await.unwrap();
    assert_eq!(a.indexes().unwrap(), b.indexes().unwrap());
    assert_eq!(a.indexes().unwrap().len(), 30);
}

/// Rewrites the property in the newest log entry, as a corrupt or hostile writer could.
async fn set_property(store: &Arc<InMemory>, table: &str, version: u64, value: &str) {
    let p = Path::from(format!("db/{table}/data/_delta_log/{version:020}.json"));
    let body = store.get(&p).await.unwrap().bytes().await.unwrap();
    let mut out = String::new();
    for line in String::from_utf8(body.to_vec()).unwrap().lines() {
        let mut v: serde_json::Value = serde_json::from_str(line).unwrap();
        if let Some(c) = v.pointer_mut("/metaData/configuration") {
            c[INDEXES_PROPERTY] = json!(value);
        }
        out.push_str(&v.to_string());
        out.push('\n');
    }
    store
        .put(&p, PutPayload::from(out.into_bytes()))
        .await
        .unwrap();
}

#[tokio::test]
async fn malformed_and_unregistered_specs() {
    let store = Arc::new(InMemory::new());
    let t = db(&store, true)
        .await
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![Some(5), None, Some(7)])),
        ],
    )
    .unwrap();
    t.append(vec![batch]).await.unwrap();
    t.create_index(sum("a")).await.unwrap();

    // A client without the kind keeps the spec and still reads and writes.
    let plain = db(&store, false).await.open_table("t").await.unwrap();
    assert_eq!(plain.indexes().unwrap().len(), 1);
    assert_eq!(plain.scan().count().await.unwrap(), 3);
    assert!(matches!(
        plain.create_index(sum("b")).await,
        Err(Error::IndexKindNotRegistered(_))
    ));

    // A malformed property does not stop the table from opening.
    set_property(&store, "t", t.version(), "[{\"name\":").await;
    let broken = db(&store, true).await.open_table("t").await.unwrap();
    assert!(matches!(broken.indexes(), Err(Error::InvalidIndexSpec(_))));
    assert_eq!(broken.scan().count().await.unwrap(), 3);
    assert!(matches!(
        broken.create_index(sum("c")).await,
        Err(Error::InvalidIndexSpec(_))
    ));
}
