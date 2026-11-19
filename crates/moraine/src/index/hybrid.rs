//! Hybrid search (D42): a vector search and a full-text search
//! against the same snapshot, each for `k * oversample` hits, fused into one ranking.
//!
//! The default fusion is reciprocal rank fusion with the constant 60 (Cormack, Clarke and
//! Büttcher, 2009): a row's score is the sum over the lists holding it of `1 / (60 + rank)`,
//! ranks counted from 1. It needs no calibration between a distance and a BM25 score, which
//! live on unrelated scales, and a row found by both searches outranks one found by either
//! alone at a similar rank. The fusion is pluggable ([`Fusion`]).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::logical_expr::Expr;
use serde_json::Value;

use super::query::{Search, SearchHit, SearchResult, SearchStats, rows_of};
use super::topk::Candidate;
use crate::error::{Error, Result};
use crate::table::TableInner;

/// Combines ranked lists of hits (best first) into one score per row; larger is better.
pub trait Fusion: Send + Sync + std::fmt::Debug {
    /// `lists[i]` is the `i`-th search's hits, best first: the vector search, then the
    /// full-text search. Returns each row's fused score; rows left out are dropped.
    fn fuse(&self, lists: &[Vec<SearchHit>]) -> Vec<(SearchHit, f32)>;
}

/// Reciprocal rank fusion: `sum 1 / (k + rank)`, ranks from 1 (see the module documentation).
#[derive(Clone, Copy, Debug)]
pub struct Rrf {
    /// The rank constant; 60 by default.
    pub k: f32,
}

impl Default for Rrf {
    fn default() -> Self {
        Rrf { k: 60.0 }
    }
}

impl Fusion for Rrf {
    fn fuse(&self, lists: &[Vec<SearchHit>]) -> Vec<(SearchHit, f32)> {
        let mut by_row: HashMap<(String, u64), (SearchHit, f32)> = HashMap::new();
        for list in lists {
            for (rank, h) in list.iter().enumerate() {
                let add = 1.0 / (self.k + rank as f32 + 1.0);
                by_row
                    .entry((h.file.clone(), h.offset))
                    .and_modify(|e| e.1 += add)
                    .or_insert((h.clone(), add));
            }
        }
        by_row.into_values().collect()
    }
}

/// A hybrid search builder: `table.search_hybrid("emb", v, "body", "object storage")`. See
/// the module documentation.
#[derive(Debug, Clone)]
pub struct HybridSearch {
    table: Arc<TableInner>,
    vector: Search,
    text: Search,
    k: usize,
    oversample: usize,
    filter: Option<Expr>,
    select: Option<Vec<String>>,
    fusion: Arc<dyn Fusion>,
}

impl HybridSearch {
    pub(crate) fn new(
        table: Arc<TableInner>,
        vector_index: &str,
        vector: Vec<f32>,
        text_index: &str,
        text: &str,
    ) -> HybridSearch {
        HybridSearch {
            vector: Search::vector(table.clone(), vector_index, vector),
            text: Search::text(table.clone(), text_index, text.to_string()),
            table,
            k: 10,
            oversample: 4,
            filter: None,
            select: None,
            fusion: Arc::new(Rrf::default()),
        }
    }

    /// Hits wanted, at most (default 10).
    pub fn limit(mut self, n: usize) -> Self {
        self.k = n;
        self
    }

    /// Each search asks for `limit * n` hits before fusion (default 4).
    pub fn oversample(mut self, n: usize) -> Self {
        self.oversample = n.max(1);
        self
    }

    /// Only rows for which `e` is true, in both searches; several calls combine with AND.
    pub fn filter(mut self, e: Expr) -> Self {
        self.filter = Some(match self.filter.take() {
            Some(f) => f.and(e),
            None => e,
        });
        self
    }

    /// The columns of the result (default: all), before the score column.
    pub fn select<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.select = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Overrides a search parameter of the vector search (`expansion_search`, `rerank`).
    pub fn vector_param(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.vector = self.vector.param(key, value);
        self
    }

    /// Overrides a search parameter of the full-text search (`boosts`, `conjunction`).
    pub fn text_param(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.text = self.text.param(key, value);
        self
    }

    /// Replaces reciprocal rank fusion.
    pub fn fusion(mut self, f: Arc<dyn Fusion>) -> Self {
        self.fusion = f;
        self
    }

    /// Runs both searches against one snapshot taken now and fuses them. The score column
    /// holds the fused score; `stats` adds up both searches.
    pub async fn collect(self) -> Result<SearchResult> {
        let snap = self.table.snapshot_for_query().await?;
        match self.clone().run(snap).await {
            Err(e) if e.is_missing_object() => {
                let fresh = self.table.refresh().await?;
                self.run(fresh).await.map_err(crate::error::Error::expired)
            }
            r => r,
        }
    }

    async fn run(self, snap: std::sync::Arc<crate::snapshot::Snapshot>) -> Result<SearchResult> {
        let want = self.k.saturating_mul(self.oversample);
        let prepare = |mut s: Search| {
            s.k = want;
            s.filter = self.filter.clone();
            s.hits_only = true;
            s
        };
        let (v, t) = (prepare(self.vector.clone()), prepare(self.text.clone()));
        let (v, t) = futures::try_join!(
            super::query::run(v, snap.clone()),
            super::query::run(t, snap.clone())
        )?;
        let mut stats = SearchStats::default();
        for s in [&v.stats, &t.stats] {
            stats.files = s.files;
            stats.skipped_files += s.skipped_files;
            stats.sidecar_files += s.sidecar_files;
            stats.exact_files += s.exact_files;
            stats.exact_rows += s.exact_rows;
            stats.rescored += s.rescored;
        }
        let mut fused = self.fusion.fuse(&[v.hits, t.hits]);
        if fused.iter().any(|(_, s)| !s.is_finite()) {
            return Err(Error::InvalidQuery(
                "the fusion returned a non-finite score".into(),
            ));
        }
        fused.sort_by(|a, b| {
            b.1.total_cmp(&a.1)
                .then_with(|| a.0.file.cmp(&b.0.file))
                .then(a.0.offset.cmp(&b.0.offset))
        });
        fused.truncate(self.k);
        let ranked: Vec<Candidate> = fused
            .into_iter()
            .map(|(h, s)| Candidate {
                file: h.file.as_str().into(),
                offset: h.offset,
                score: s,
                exact: true,
            })
            .collect();
        let schema = snap.schema().clone();
        let output: Vec<String> = match &self.select {
            Some(c) => c.clone(),
            None => schema.fields().iter().map(|f| f.name().clone()).collect(),
        };
        for c in &output {
            if schema.field_with_name(c).is_err() {
                return Err(crate::schema::column_not_found(c, &schema));
            }
        }
        let out_schema = crate::schema::project_schema(&schema, &output)?;
        let (batch, hits) = rows_of(&self.table, &snap, &output, &out_schema, ranked).await?;
        Ok(SearchResult { batch, hits, stats })
    }
}
