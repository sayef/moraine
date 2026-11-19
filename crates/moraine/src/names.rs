//! Not-found errors that say what exists: the closest name ("did you mean") and the available
//! names, so a misspelt table or column is fixed from the message alone.

use std::fmt;

/// How many available names a message lists before it summarises the rest.
const LISTED: usize = 20;

/// A name that does not exist, with the closest existing name and the names that do exist.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct NotFound {
    /// The name that was asked for.
    pub name: String,
    /// The closest available name, when one is close enough to be a likely typo.
    pub suggestion: Option<String>,
    /// Every available name, sorted.
    pub available: Vec<String>,
}

impl NotFound {
    /// A name with no information about what exists (nothing could be listed).
    pub fn bare(name: impl Into<String>) -> Self {
        NotFound {
            name: name.into(),
            ..NotFound::default()
        }
    }

    /// A name and the names that exist; picks the suggestion.
    pub fn among<I, S>(name: impl Into<String>, available: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let name = name.into();
        let mut available: Vec<String> = available.into_iter().map(Into::into).collect();
        available.sort();
        available.dedup();
        let suggestion = closest(&name, available.iter().map(String::as_str)).map(str::to_owned);
        NotFound {
            name,
            suggestion,
            available,
        }
    }
}

impl fmt::Display for NotFound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.name)?;
        if let Some(s) = &self.suggestion {
            write!(f, "; did you mean {s:?}?")?;
        }
        if self.available.is_empty() {
            return Ok(());
        }
        f.write_str("; available: ")?;
        for (i, n) in self.available.iter().take(LISTED).enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            f.write_str(n)?;
        }
        if self.available.len() > LISTED {
            write!(f, " and {} more", self.available.len() - LISTED)?;
        }
        Ok(())
    }
}

/// The candidate closest to `name`, if it is close enough to be a typo: equal ignoring case, or
/// within an edit distance of a third of the name's length (at least 1, at most 3). Ties go to
/// the alphabetically first candidate, so the answer does not depend on listing order.
pub fn closest<'a>(name: &str, candidates: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    let limit = (name.chars().count() / 3).clamp(1, 3);
    let lower = name.to_lowercase();
    let mut best: Option<(usize, &'a str)> = None;
    for c in candidates {
        if c == name {
            continue;
        }
        let d = if c.to_lowercase() == lower {
            0
        } else {
            edit_distance(&lower, &c.to_lowercase())
        };
        if d > limit {
            continue;
        }
        if best.is_none_or(|(bd, bc)| d < bd || (d == bd && c < bc)) {
            best = Some((d, c));
        }
    }
    best.map(|(_, c)| c)
}

/// Optimal string alignment distance (Levenshtein plus adjacent transpositions), on chars.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let w = b.len() + 1;
    let mut d = vec![0usize; (a.len() + 1) * w];
    for i in 0..=a.len() {
        d[i * w] = i;
    }
    for (j, cell) in d.iter_mut().enumerate().take(w) {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            let mut v = (d[(i - 1) * w + j] + 1)
                .min(d[i * w + j - 1] + 1)
                .min(d[(i - 1) * w + j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                v = v.min(d[(i - 2) * w + j - 2] + 1);
            }
            d[i * w + j] = v;
        }
    }
    d[a.len() * w + b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distances() {
        assert_eq!(edit_distance("metrics", "metrics"), 0);
        assert_eq!(edit_distance("metrcs", "metrics"), 1);
        assert_eq!(edit_distance("metric", "metrics"), 1);
        assert_eq!(edit_distance("", "abc"), 3);
        assert_eq!(edit_distance("kitten", "sitting"), 3);
    }

    #[test]
    fn suggests_typos_only() {
        let names = ["metrics", "params", "runs", "series"];
        assert_eq!(closest("metrcs", names), Some("metrics"));
        assert_eq!(closest("Runs", names), Some("runs"));
        assert_eq!(closest("param", names), Some("params"));
        assert_eq!(closest("blobs", names), None);
        assert_eq!(
            closest("x", ["y", "z"]),
            Some("y"),
            "ties go to the first name"
        );
        assert_eq!(
            closest("runs", names),
            None,
            "an exact match is not a suggestion"
        );
    }

    #[test]
    fn message_lists_names() {
        let nf = NotFound::among("metrcs", ["series", "metrics", "runs"]);
        assert_eq!(
            nf.to_string(),
            r#""metrcs"; did you mean "metrics"?; available: metrics, runs, series"#
        );
        assert_eq!(NotFound::bare("t").to_string(), r#""t""#);
        let many = NotFound::among("zzz", (0..25).map(|i| format!("t{i:02}")));
        assert!(many.to_string().ends_with("t19 and 5 more"));
        assert!(many.suggestion.is_none());
    }
}
