# Full-text and hybrid search

This guide adds a full-text index to a table, searches it, and combines it with a vector index.
How sidecars are built, cached and found is in [Sidecar indexes](../concepts/indexes.md); this
page covers what is particular to text.

## Enable the kind

The full-text kind, `fts`, is behind a cargo feature; hybrid search needs a vector kind too:

```toml
[dependencies]
morainedb = { version = "0.1.0-beta.2", features = ["fts", "vector"] }
serde_json = "1"                                                         # index parameters
```

`fts` builds one [tantivy](https://github.com/quickwit-oss/tantivy) index per data file. Every
client that searches or builds a full-text index needs the feature.

## Create the index

A full-text index covers one or more string columns (`Utf8`, `LargeUtf8` or `Utf8View`). Null
and empty values are fine: every row is a document, so document counts always match row counts.

```rust
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
```

`tokenizer` and `record` at the top of `params` apply to every column; `fields.<column>` sets
them for one. `create_index` stores every column's settings, so the stored spec never changes
meaning.

| Where | Key | Default | Meaning |
|---|---|---|---|
| `params` | `tokenizer` | `default` | How text becomes terms (see [Tokenizers](#tokenizers)) |
| `params` | `record` | `positions` | `basic` (documents only), `freqs` (term frequencies), `positions` (needed by phrase queries) |
| `params` | `max_indexed_chars` | 1,000,000 | Longer values are cut for indexing; the stored value is untouched |
| `policy` | `min_rows_for_sidecar` | 1,000 | Smaller files get no sidecar and are indexed in memory when searched |
| `search` | `boosts` | 1.0 per column | Multiplies a column's scores |
| `search` | `conjunction` | `false` | `true` makes every word required |

`params` change what a sidecar holds and are hashed; `policy` and `search` are not, so changing
a boost needs no rebuild. Column names must be usable in the query syntax: no whitespace, no
`:`, `.`, `^`, quotes or brackets, and no leading `-`, `_` or `+`.

## Search

```rust
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
```

The query uses tantivy's syntax: words (any may match), `"a phrase"`, `title:word`, `+required`,
`-excluded`, `AND`, `OR`, parentheses and `word^2` for a boost. A query that is empty, or that
holds nothing once tokenised, or only excluded words, returns no rows. A syntax error, a column
the index does not hold, or a phrase on a column that records no positions is
`Error::InvalidQuery`, naming the problem.

The result has the same shape as a vector search: the rows, best first, the hits (data file,
row offset, score) and how the files were searched (`stats.sidecar_files`, and
`stats.exact_files` for files indexed in memory).

## How ranking works

BM25 scores a row with statistics of the whole collection: how many documents there are, their
average length, and how many hold each query word. moraine keeps one index per data file, but it
does not score with each file's own statistics. A search first gets a searcher for every file of
the snapshot, adds up their statistics, and scores every file with the sums. The scores are then
those of one tantivy index over the whole table, whatever the number of files and whichever of
them have sidecars.

The test suite checks this against such a merged index on random corpora: every score agrees
within 1e-5 and the rankings are the same, only rows with exactly equal scores may be ordered
differently (moraine breaks ties by data file, then row). See
[Benchmarks](../benchmarks.md#full-text-search) for the numbers.

A filter does not change the statistics, just as it would not change a merged index's: it only
drops rows. A file whose statistics rule the filter out is not searched, but still counts in the
statistics.

## Files without a sidecar

Until `build_indexes` has built a file's sidecar, and always for files below
`min_rows_for_sidecar`, a search indexes the file's text columns in memory with the same builder,
so the answer is the same either way. The in-memory indexes are kept for later searches up to
`DatabaseBuilder::fts_memory` (default 512 MiB); beyond it the least recently used are dropped,
and a query never fails for it.

Sidecars are built as for vectors:

```rust
table.build_indexes(BuildOptions::default()).await?;
```

A build holds tantivy's writer memory (32 MiB) and spills larger files to temporary segments on
disk, merged into one segment at the end. Rebuilding the same rows gives a byte-identical sidecar.

## Tokenizers

| Name | Terms |
|---|---|
| `default` | Words (runs of letters and digits), longer than 40 bytes dropped, lowercased |
| `raw` | The whole value as one term (identifiers, tags) |
| `whitespace` | Split on whitespace, lowercased |
| `en_stem`, `de_stem`, `fr_stem`, ... | `default`, then the Snowball stemmer of the language: `ar da de el en es fi fr hu it nl no pt ro ru sv ta tr` |
| `ngram2`, `ngram3` | Lowercased character pairs or triples, for languages written without spaces |

A tokenizer of your own is a tantivy `TextAnalyzer` registered under a name. Every client that
builds or searches an index naming it must register the same tokenizer under the same name; a
client that does not know a name gets `Error::TokenizerNotRegistered` rather than different
terms. Built-in names cannot be registered again.

```rust
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
```

## Hybrid search

`search_hybrid` runs a vector search and a full-text search against the same snapshot and fuses
their rankings:

```rust
let result = docs
    .search_hybrid("emb", vec![1.0, 0.1], "text", "storage")
    .limit(3)
    .select(["id", "body"])
    .collect()
    .await?;
```

Each search asks for `k * oversample` hits (default 4, `.oversample(n)`), with the same filter.
The fusion is reciprocal rank fusion with the constant 60: a row's score is the sum, over the
lists it appears in, of `1 / (60 + rank)`. I chose it because it needs no calibration between a
vector distance and a BM25 score, which live on unrelated scales, and because a row both searches
find outranks one that either finds alone at a similar rank. `_moraine_score` holds the fused
score. `.vector_param` and `.text_param` override each search's defaults, and `.fusion` takes any
`moraine::index::hybrid::Fusion`, for example a weighted sum of normalised scores.

## Fresh results without waiting

With [`Consistency::Background`](../concepts/consistency.md#background-refresh), a task per table
handle picks up new commits every interval and prepares the caches for the new files, including
their full-text searchers, so the first search after another writer's commit makes no request
for the snapshot and indexes nothing:

```rust
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
```
