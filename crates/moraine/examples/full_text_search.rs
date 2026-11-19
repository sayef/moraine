//! The full-text and hybrid search code of the documentation, compiled by
//! `cargo clippy --all-targets --all-features` and runnable with
//! `cargo run -p morainedb --features fts,vector --example full_text_search`.
//!
//! `main` is the program of the "Search text" section of docs/getting-started.md, kept
//! identical to it. The functions after it hold the snippets of
//! docs/guides/full-text-search.md, each as written on that page, and `main` runs them.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::print_batches;
use moraine::index::BuildOptions;
use moraine::{Database, IndexSpec, TableOptions};

#[tokio::main]
async fn main() -> moraine::Result<()> {
    let _ = std::fs::remove_dir_all("moraine-text");
    let db = Database::builder("moraine-text").open().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ]));
    let notes = db
        .create_table("notes", schema.clone(), TableOptions::default())
        .await?;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                "Glaciers",
                "Object storage",
                "Moraines",
            ])),
            Arc::new(StringArray::from(vec![
                "Ice moves slowly down the valley.",
                "Writers commit with one conditional put.",
                "A moraine is rock and soil a glacier left behind.",
            ])),
        ],
    )
    .expect("columns match the schema");
    notes.append(vec![batch]).await?;

    // Every column of a full-text index is a string column; null and empty values are fine.
    notes
        .create_index(IndexSpec::new("text", "fts", &["title", "body"]))
        .await?;
    notes.build_indexes(BuildOptions::default()).await?;

    let result = notes
        .search_text("text", "glacier valley")
        .limit(2)
        .select(["id", "title"])
        .collect()
        .await?;
    print_batches(&[result.batch]).expect("printable");

    guide::run(&db, &notes).await
}

/// The snippets of docs/guides/full-text-search.md.
mod guide {
    use moraine::{Database, Table};

    pub async fn run(db: &Database, notes: &Table) -> moraine::Result<()> {
        create(notes).await?;
        search(notes).await?;
        build(notes).await?;
        tokenizer("moraine-text").await?;
        hybrid(db).await?;
        background("moraine-text").await?;
        Ok(())
    }

    // "Create the index"
    async fn create(table: &Table) -> moraine::Result<()> {
        use moraine::IndexSpec;
        use serde_json::json;

        let spec = IndexSpec::new("text_en", "fts", &["title", "body"])
            .params(json!({
                "tokenizer": "en_stem",
                "fields": {"title": {"record": "freqs"}}
            }))
            .search(json!({"boosts": {"title": 2.0}}));
        let resolved = table.create_index(spec).await?;
        println!("{}", resolved.params);
        Ok(())
    }

    // "Search"
    async fn search(table: &Table) -> moraine::Result<()> {
        use moraine::expr::{col, lit};

        let result = table
            .search_text("text", "glacier OR \"conditional put\"")
            .limit(10)
            .filter(col("id").gt(lit(0i64)))
            .select(["id", "title"])
            .collect()
            .await?;
        // Rows best first, with `_moraine_score`: the BM25 score (larger is better).
        for hit in &result.hits {
            println!("{} @ {}: {}", hit.file, hit.offset, hit.score);
        }

        // Search defaults can be overridden per query.
        let strict = table
            .search_text("text", "glacier rock")
            .param("conjunction", true)
            .param("boosts", serde_json::json!({"title": 3.0}))
            .collect()
            .await?;
        println!("{} rows hold both words", strict.batch.num_rows());
        Ok(())
    }

    // "Files without a sidecar"
    async fn build(table: &Table) -> moraine::Result<()> {
        use moraine::index::BuildOptions;

        table.build_indexes(BuildOptions::default()).await?;
        Ok(())
    }

    // "Tokenizers"
    async fn tokenizer(location: &str) -> moraine::Result<()> {
        use moraine::Database;
        use moraine::index::fts::tantivy::tokenizer::{
            LowerCaser, TextAnalyzer, WhitespaceTokenizer,
        };

        let words = TextAnalyzer::builder(WhitespaceTokenizer::default())
            .filter(LowerCaser)
            .build();
        let db = Database::builder(location)
            .tokenizer("words", words)
            .open()
            .await?;
        let _ = db;
        Ok(())
    }

    // "Hybrid search"
    async fn hybrid(db: &Database) -> moraine::Result<()> {
        use std::sync::Arc;

        use arrow::array::{Float32Builder, Int64Array, ListBuilder, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use moraine::index::BuildOptions;
        use moraine::{IndexSpec, TableOptions};
        use serde_json::json;

        // A table with text and vectors, and an index on each.
        let element = Arc::new(Field::new("element", DataType::Float32, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
            Field::new("embedding", DataType::List(element.clone()), true),
        ]));
        let docs = db
            .create_table("docs", schema.clone(), TableOptions::default())
            .await?;
        let mut v = ListBuilder::new(Float32Builder::new()).with_field(element);
        for x in [[1.0, 0.0], [0.0, 1.0], [0.7, 0.7]] {
            v.values().append_slice(&x);
            v.append(true);
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    "object storage",
                    "vector search",
                    "storage for vectors",
                ])),
                Arc::new(v.finish()),
            ],
        )
        .expect("columns match the schema");
        docs.append(vec![batch]).await?;
        docs.create_index(
            IndexSpec::new("emb", "vec-hnsw", &["embedding"]).params(json!({"dim": 2})),
        )
        .await?;
        docs.create_index(IndexSpec::new("text", "fts", &["body"]))
            .await?;
        docs.build_indexes(BuildOptions::default()).await?;

        let result = docs
            .search_hybrid("emb", vec![1.0, 0.1], "text", "storage")
            .limit(3)
            .select(["id", "body"])
            .collect()
            .await?;
        arrow::util::pretty::print_batches(&[result.batch]).expect("printable");
        Ok(())
    }

    // "Fresh results without waiting" (background refresh)
    async fn background(location: &str) -> moraine::Result<()> {
        use std::time::Duration;

        use moraine::{Consistency, Database};

        let db = Database::builder(location)
            .consistency(Consistency::Background {
                interval: Duration::from_secs(1),
            })
            .open()
            .await?;
        let notes = db.open_table("notes").await?;
        // Queries use the snapshot the background task keeps current: no request for it.
        let hits = notes.search_text("text", "moraine").collect().await?;
        println!("{} hits at version {}", hits.hits.len(), notes.version());
        Ok(())
    }
}
