//! The global top-k merge (cases V10 and V15): candidates from every file are
//! ordered by score (as the kind orders it), then data file path, then row offset, so ties are
//! deterministic and identical values in several rows stay separate hits.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use super::kind::Order;

/// One candidate of a search.
#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    /// The data file's path, relative to the table's data root.
    pub file: Arc<str>,
    /// The row's position in the file.
    pub offset: u64,
    /// The kind's score.
    pub score: f32,
    /// Whether the score is exact (the exact route or a rescore) rather than approximate.
    pub exact: bool,
}

/// `a` before `b` in the result.
pub fn cmp(order: Order, a: &Candidate, b: &Candidate) -> Ordering {
    let s = match order {
        Order::Ascending => a.score.total_cmp(&b.score),
        Order::Descending => b.score.total_cmp(&a.score),
    };
    s.then_with(|| a.file.cmp(&b.file))
        .then_with(|| a.offset.cmp(&b.offset))
}

struct Ranked {
    order: Order,
    c: Candidate,
}

impl PartialEq for Ranked {
    fn eq(&self, o: &Self) -> bool {
        cmp(self.order, &self.c, &o.c) == Ordering::Equal
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Ranked {
    /// The heap's top is the worst kept candidate.
    fn cmp(&self, o: &Self) -> Ordering {
        cmp(self.order, &self.c, &o.c)
    }
}

/// Keeps the best `k` candidates pushed into it.
pub struct TopK {
    order: Order,
    k: usize,
    heap: BinaryHeap<Ranked>,
}

impl TopK {
    /// Keeps the best `k` in `order`.
    pub fn new(order: Order, k: usize) -> TopK {
        TopK {
            order,
            k,
            heap: BinaryHeap::with_capacity(k.min(1 << 16) + 1),
        }
    }

    /// Offers a candidate; it is kept if it is among the best `k` so far.
    pub fn push(&mut self, c: Candidate) {
        if self.k == 0 {
            return;
        }
        if self.heap.len() < self.k {
            self.heap.push(Ranked {
                order: self.order,
                c,
            });
            return;
        }
        if let Some(top) = self.heap.peek()
            && cmp(self.order, &c, &top.c) == Ordering::Less
        {
            self.heap.pop();
            self.heap.push(Ranked {
                order: self.order,
                c,
            });
        }
    }

    /// Best first.
    pub fn into_sorted(self) -> Vec<Candidate> {
        let mut v: Vec<Candidate> = self.heap.into_iter().map(|r| r.c).collect();
        let order = self.order;
        v.sort_by(|a, b| cmp(order, a, b));
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(file: &str, offset: u64, score: f32) -> Candidate {
        Candidate {
            file: file.into(),
            offset,
            score,
            exact: true,
        }
    }

    #[test]
    fn merge_of_per_file_topk_equals_global_sort() {
        // A deterministic pseudo-random set with many ties.
        let mut all = Vec::new();
        let mut x = 7u64;
        for f in 0..5 {
            for o in 0..200 {
                x = x
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                all.push(c(&format!("f{f}"), o, ((x >> 33) % 50) as f32));
            }
        }
        for order in [Order::Ascending, Order::Descending] {
            for k in [0, 1, 7, 100, 5000] {
                let mut sorted = all.clone();
                sorted.sort_by(|a, b| cmp(order, a, b));
                sorted.truncate(k);
                // Per file top-k, then merged.
                let mut global = TopK::new(order, k);
                for f in 0..5 {
                    let name = format!("f{f}");
                    let mut per = TopK::new(order, k);
                    for x in all.iter().filter(|x| *x.file == *name) {
                        per.push(x.clone());
                    }
                    for x in per.into_sorted() {
                        global.push(x);
                    }
                }
                assert_eq!(global.into_sorted(), sorted, "{order:?} k={k}");
            }
        }
    }

    #[test]
    fn ties_break_by_path_then_offset_v10_v15() {
        let mut t = TopK::new(Order::Ascending, 3);
        for x in [
            c("b", 1, 0.5),
            c("a", 9, 0.5),
            c("a", 2, 0.5),
            c("a", 2, 0.1),
        ] {
            t.push(x);
        }
        let got: Vec<(String, u64)> = t
            .into_sorted()
            .into_iter()
            .map(|x| (x.file.to_string(), x.offset))
            .collect();
        assert_eq!(
            got,
            vec![("a".into(), 2), ("a".into(), 2), ("a".into(), 9)],
            "identical scores stay separate hits"
        );
        // NaN never outranks a number under total_cmp in ascending order.
        let mut t = TopK::new(Order::Ascending, 1);
        t.push(c("a", 0, f32::NAN));
        t.push(c("a", 1, 1.0));
        assert_eq!(t.into_sorted()[0].offset, 1);
    }
}
