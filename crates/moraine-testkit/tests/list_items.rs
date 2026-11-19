//! List columns whose item field is not named `element` (D39): Arrow names it `item` by
//! default, Delta `element`. moraine renames it at creation and on write, so a batch built with
//! Arrow's defaults appends, reads back, and delta-rs reads the table.

use std::sync::Arc;

use arrow::array::{Array, AsArray, Float32Builder, Int64Array, ListBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Float32Type, Schema};
use datafusion::prelude::SessionContext;
use moraine::{Database, TableOptions};

fn list(item: &str) -> DataType {
    DataType::List(Arc::new(Field::new(item, DataType::Float32, true)))
}

fn schema(item: &str) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", list(item), true),
    ]))
}

/// Rows `lo..hi` with `v = [id, id + 0.5]`, every third null, built with Arrow's default item
/// field name (`item`) unless `item` says otherwise.
fn batch(lo: i64, hi: i64, item: &str) -> RecordBatch {
    let mut b = ListBuilder::new(Float32Builder::new()).with_field(Arc::new(Field::new(
        item,
        DataType::Float32,
        true,
    )));
    for i in lo..hi {
        if i % 3 == 0 {
            b.append_null();
        } else {
            b.values().append_value(i as f32);
            b.values().append_value(i as f32 + 0.5);
            b.append(true);
        }
    }
    RecordBatch::try_new(
        schema(item),
        vec![
            Arc::new(Int64Array::from_iter_values(lo..hi)),
            Arc::new(b.finish()),
        ],
    )
    .unwrap()
}

fn values(batches: &[RecordBatch]) -> Vec<(i64, Option<Vec<f32>>)> {
    let mut out = Vec::new();
    for b in batches {
        let id = b
            .column_by_name("id")
            .unwrap()
            .as_primitive::<arrow::datatypes::Int64Type>();
        let v = b.column_by_name("v").unwrap().as_list::<i32>();
        for r in 0..b.num_rows() {
            let x =
                (!v.is_null(r)).then(|| v.value(r).as_primitive::<Float32Type>().values().to_vec());
            out.push((id.value(r), x));
        }
    }
    out.sort_by_key(|x| x.0);
    out
}

#[tokio::test]
async fn arrow_default_item_name_appends_and_reads_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::builder(dir.path().to_str().unwrap())
        .open()
        .await
        .unwrap();
    // Created with Arrow's default item name: stored as Delta's.
    let t = db
        .create_table("t", schema("item"), TableOptions::default())
        .await
        .unwrap();
    assert_eq!(
        t.schema().field_with_name("v").unwrap().data_type(),
        &list("element")
    );
    // Batches with either name, or any other, append.
    t.append(vec![batch(0, 10, "item")]).await.unwrap();
    t.append(vec![batch(10, 20, "element")]).await.unwrap();
    t.append(vec![batch(20, 25, "value")]).await.unwrap();
    let got = values(&t.scan().collect().await.unwrap());
    let want = values(&[
        batch(0, 10, "item"),
        batch(10, 20, "item"),
        batch(20, 25, "item"),
    ]);
    assert_eq!(got, want);
    // Creating it again with the other spelling is the same table.
    db.create_table("t", schema("element"), TableOptions::default())
        .await
        .unwrap();
    // A column added with Arrow's default name is stored as Delta's.
    t.add_column(Field::new("w", list("item"), true))
        .await
        .unwrap();
    assert_eq!(
        t.schema().field_with_name("w").unwrap().data_type(),
        &list("element")
    );

    // delta-rs reads the table.
    let url = url::Url::from_directory_path(dir.path().join("t/data")).unwrap();
    let table = deltalake::open_table(url).await.unwrap();
    let ctx = SessionContext::new();
    let theirs = ctx
        .read_table(table.table_provider().await.unwrap())
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows = values(&theirs);
    assert_eq!(rows, want);
}
