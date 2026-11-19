//! The vector-search code of the documentation, compiled by `cargo clippy --all-targets` and
//! runnable with `cargo run -p morainedb --features vector --example vector_search`.
//!
//! `main` is the program of the "Search vectors" section of docs/getting-started.md, kept
//! identical to it. The functions after it hold the snippets of docs/guides/vector-search.md,
//! each as written on that page, and `main` runs them on the same table.

use std::sync::Arc;

use arrow::array::{Float32Builder, Int64Array, ListBuilder, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::print_batches;
use moraine::expr::{col, lit};
use moraine::index::BuildOptions;
use moraine::{Database, IndexSpec, TableOptions};
use serde_json::json;

#[tokio::main]
async fn main() -> moraine::Result<()> {
    let _ = std::fs::remove_dir_all("moraine-vectors");
    let db = Database::builder("moraine-vectors").open().await?;

    // A vector column is a list of Float32; Delta names the list's item field `element`.
    let element = Arc::new(Field::new("element", DataType::Float32, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("lang", DataType::Utf8, true),
        Field::new("embedding", DataType::List(element.clone()), true),
    ]));
    let docs = db
        .create_table("docs", schema.clone(), TableOptions::default())
        .await?;

    let mut vectors = ListBuilder::new(Float32Builder::new()).with_field(element);
    for v in [
        [1.0, 0.0, 0.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.9, 0.1, 0.0, 0.0],
    ] {
        vectors.values().append_slice(&v);
        vectors.append(true);
    }
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["en", "de", "en"])),
            Arc::new(vectors.finish()),
        ],
    )
    .expect("columns match the schema");
    docs.append(vec![batch]).await?;

    // The dimension and the metric belong to the index, not the column.
    let spec = IndexSpec::new("emb", "vec-hnsw", &["embedding"])
        .params(json!({"dim": 4, "metric": "cosine"}));
    docs.create_index(spec).await?;
    docs.build_indexes(BuildOptions::default()).await?;

    let result = docs
        .search_vector("emb", vec![1.0, 0.0, 0.0, 0.0])
        .limit(2)
        .filter(col("lang").eq(lit("en")))
        .select(["id"])
        .collect()
        .await?;
    print_batches(&[result.batch]).expect("printable");

    guide::run(&db, &docs).await
}

/// The snippets of docs/guides/vector-search.md.
mod guide {
    use moraine::{Database, Table, TableOptions};

    pub async fn run(db: &Database, docs: &Table) -> moraine::Result<()> {
        // The guide's own table, with a 384-dimension index.
        let articles = articles(db).await?;
        create(&articles).await?;
        change(&articles).await?;
        // The table of `main`, whose vectors have 4 dimensions.
        search(docs, vec![1.0, 0.0, 0.0, 0.0]).await?;
        build(docs).await?;
        before_commit(&articles).await?;
        cache("moraine-vectors").await?;
        Ok(())
    }

    // "The vector column"
    async fn articles(db: &Database) -> moraine::Result<Table> {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let element = Arc::new(Field::new("element", DataType::Float32, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("lang", DataType::Utf8, true),
            Field::new("embedding", DataType::List(element), true),
        ]));
        db.create_table("articles", schema, TableOptions::default())
            .await
    }

    // "Create the index"
    async fn create(table: &Table) -> moraine::Result<()> {
        use moraine::IndexSpec;
        use serde_json::json;

        let spec = IndexSpec::new("emb", "vec-hnsw", &["embedding"])
            .params(json!({"dim": 384, "metric": "cosine"}));
        let resolved = table.create_index(spec).await?;
        println!("{}", resolved.search);
        Ok(())
    }

    // "Change or drop an index"
    async fn change(table: &Table) -> moraine::Result<()> {
        use moraine::IndexSpec;
        use serde_json::json;

        let spec = IndexSpec::new("emb", "vec-hnsw", &["embedding"])
            .params(json!({"dim": 384, "metric": "cosine", "quantization": "f16"}));
        table.replace_index(spec).await?;

        table.drop_index("emb").await?;
        Ok(())
    }

    // "Search"
    async fn search(table: &Table, query_vector: Vec<f32>) -> moraine::Result<()> {
        use moraine::expr::{col, lit};

        let result = table
            .search_vector("emb", query_vector)
            .limit(10)
            .filter(col("lang").eq(lit("en")))
            .select(["id", "lang"])
            .collect()
            .await?;

        // Rows best first: the selected columns plus `_moraine_score`, a distance (smaller is closer).
        println!("{} rows", result.batch.num_rows());
        // Where each hit came from: (data file, row offset, score).
        for hit in &result.hits {
            println!("{} @ {}: {}", hit.file, hit.offset, hit.score);
        }
        // How the search was answered.
        println!(
            "{} files from sidecars, {} exact, {} skipped",
            result.stats.sidecar_files, result.stats.exact_files, result.stats.skipped_files
        );
        Ok(())
    }

    // "Build the sidecars"
    async fn build(table: &Table) -> moraine::Result<()> {
        use moraine::index::BuildOptions;

        let report = table.build_indexes(BuildOptions::default()).await?;
        println!(
            "built {}, already present {}",
            report.built, report.already_present
        );

        for status in table.index_status().await? {
            println!(
                "{}: {} of {} files covered",
                status.name, status.files_covered, status.files_total
            );
        }
        Ok(())
    }

    // "Index before commit"
    async fn before_commit(articles: &Table) -> moraine::Result<()> {
        use moraine::IndexSpec;
        use serde_json::json;

        // Build sidecars of appended files of up to 200,000 rows before their commit; larger
        // appends leave theirs to the build job.
        let spec = IndexSpec::new("embedding_bc", "vec-hnsw", &["embedding"])
            .params(json!({"dim": 384, "metric": "cosine"}))
            .policy(json!({
                "min_rows_for_ann": 10_000,
                "before_commit": true,
                "before_commit_max_rows": 200_000,
                "before_commit_max_build_bytes": 1u64 << 30
            }));
        articles.create_index(spec).await?;
        for s in articles.index_status().await? {
            println!(
                "{}: {} rows pending, {:.1} % on the exact route",
                s.name,
                s.rows_pending(),
                100.0 * s.exact_share()
            );
        }
        Ok(())
    }

    // "Caching sidecars", with a local directory in place of the guide's bucket.
    async fn cache(location: &str) -> moraine::Result<()> {
        use moraine::index::CacheMode;

        let db = Database::builder(location)
            .sidecar_cache(CacheMode::Disk {
                dir: std::env::temp_dir().join("moraine-sidecars"),
                max_bytes: 20 << 30,
            })
            .open()
            .await?;
        drop(db);
        Ok(())
    }
}
