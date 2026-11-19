//! Index specs, their canonical JSON and hash, and the `moraine.indexes` table property
//! (D33).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::bundle::BUNDLE_VERSION;
use super::kind::IndexKind;
use crate::error::{Error, Result};

/// The table property holding the index specs, a JSON list.
pub const INDEXES_PROPERTY: &str = "moraine.indexes";

/// One index of a table.
///
/// `params` change what a sidecar contains and are hashed; `policy` (which files get a
/// sidecar) and `search` (defaults a query may override) are not. A kind fills every default
/// when the index is created ([`IndexKind::resolve`]), so a stored spec is frozen: a later
/// release that adds a parameter treats its absence as the old behaviour.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct IndexSpec {
    /// Unique per table; matches `[a-z0-9][a-z0-9_-]{0,63}`.
    pub name: String,
    /// The registered kind: `"vec-hnsw"`, `"vec-tq"`, or one registered by the application.
    pub kind: String,
    /// The indexed columns, in the order the kind expects them.
    pub columns: Vec<String>,
    /// What the sidecar holds (hashed into the spec hash).
    #[serde(default)]
    pub params: Value,
    /// Which files get a sidecar, such as `min_rows_for_ann` (not hashed).
    #[serde(default)]
    pub policy: Value,
    /// Defaults for queries, which a query may override (not hashed).
    #[serde(default)]
    pub search: Value,
}

impl IndexSpec {
    /// A spec with empty params, policy and search defaults.
    pub fn new(name: &str, kind: &str, columns: &[&str]) -> IndexSpec {
        IndexSpec {
            name: name.into(),
            kind: kind.into(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            params: Value::Null,
            policy: Value::Null,
            search: Value::Null,
        }
    }

    /// Sets the parameters, which change what a sidecar contains.
    pub fn params(mut self, params: Value) -> IndexSpec {
        self.params = params;
        self
    }

    /// Sets the policy: which files get a sidecar.
    pub fn policy(mut self, policy: Value) -> IndexSpec {
        self.policy = policy;
        self
    }

    /// Sets the search defaults a query may override.
    pub fn search(mut self, search: Value) -> IndexSpec {
        self.search = search;
        self
    }

    /// `params[key]`, for kinds.
    pub fn param(&self, key: &str) -> Option<&Value> {
        self.params.get(key)
    }
}

/// Index names follow the table-name pattern.
pub fn validate_index_name(name: &str) -> Result<()> {
    let b = name.as_bytes();
    let first = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    let rest = |c: u8| first(c) || c == b'_' || c == b'-';
    if !b.is_empty() && b.len() <= 64 && first(b[0]) && b[1..].iter().all(|c| rest(*c)) {
        Ok(())
    } else {
        Err(Error::InvalidIndexSpec(format!(
            "index name {name:?} does not match [a-z0-9][a-z0-9_-]{{0,63}}"
        )))
    }
}

/// JSON with object keys sorted, no whitespace, integers as integers.
pub fn canonical_json(v: &Value) -> String {
    let mut out = String::new();
    write_canonical(v, &mut out);
    out
}

fn write_canonical(v: &Value, out: &mut String) {
    match v {
        Value::Object(m) => {
            let sorted: BTreeMap<&String, &Value> = m.iter().collect();
            out.push('{');
            for (i, (k, v)) in sorted.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(k.clone()).to_string());
                out.push(':');
                write_canonical(v, out);
            }
            out.push('}');
        }
        Value::Array(a) => {
            out.push('[');
            for (i, v) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(v, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

/// The spec hash: the first 16 hex characters of SHA-256 over the canonical JSON of the bundle
/// format version, the kind, its format version, the columns, the params and the library
/// fingerprint. A change to any of them yields a new hash and so new sidecars.
pub fn spec_hash(kind: &dyn IndexKind, spec: &IndexSpec) -> String {
    let v = serde_json::json!({
        "bundle": BUNDLE_VERSION,
        "kind": spec.kind,
        "kindFormat": kind.format_version(),
        "columns": spec.columns,
        "params": spec.params,
        "lib": kind.library_fingerprint(),
    });
    let digest = Sha256::digest(canonical_json(&v).as_bytes());
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// The specs a table's properties hold. `Ok(empty)` without the property;
/// [`Error::InvalidIndexSpec`] when it is malformed (the table still opens and scans).
pub fn parse_specs(props: &BTreeMap<String, String>) -> Result<Vec<IndexSpec>> {
    let Some(raw) = props.get(INDEXES_PROPERTY) else {
        return Ok(Vec::new());
    };
    let specs: Vec<IndexSpec> = serde_json::from_str(raw)
        .map_err(|e| Error::InvalidIndexSpec(format!("table property {INDEXES_PROPERTY}: {e}")))?;
    let mut names = std::collections::HashSet::new();
    for s in &specs {
        validate_index_name(&s.name)?;
        if !names.insert(s.name.as_str()) {
            return Err(Error::InvalidIndexSpec(format!(
                "index {:?} defined twice",
                s.name
            )));
        }
        if s.kind.is_empty() || s.kind.len() > super::bundle::MAX_KIND_NAME {
            return Err(Error::InvalidIndexSpec(format!(
                "index {:?} has an invalid kind {:?}",
                s.name, s.kind
            )));
        }
    }
    Ok(specs)
}

/// The property value for `specs`.
pub fn write_specs(specs: &[IndexSpec]) -> String {
    serde_json::to_string(specs).unwrap_or_else(|_| "[]".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_json_is_order_independent() {
        let a: Value = serde_json::from_str(r#"{"b":1,"a":{"y":[1,2.5,"x"],"x":null}}"#).unwrap();
        let b: Value =
            serde_json::from_str(r#"{ "a" : {"x":null, "y":[1, 2.5, "x"]}, "b": 1 }"#).unwrap();
        assert_eq!(canonical_json(&a), canonical_json(&b));
        assert_eq!(
            canonical_json(&a),
            r#"{"a":{"x":null,"y":[1,2.5,"x"]},"b":1}"#
        );
        assert_eq!(canonical_json(&json!(16)), "16");
        assert_eq!(canonical_json(&json!("é\"")), "\"é\\\"\"");
    }

    #[test]
    fn names() {
        assert!(validate_index_name("emb_v2").is_ok());
        assert!(validate_index_name("").is_err());
        assert!(validate_index_name("Emb").is_err());
        assert!(validate_index_name("_moraine_key").is_err());
        assert!(validate_index_name(&"a".repeat(65)).is_err());
    }

    #[test]
    fn malformed_properties() {
        let mut p = BTreeMap::new();
        assert!(parse_specs(&p).unwrap().is_empty());
        p.insert(INDEXES_PROPERTY.into(), "[".into());
        assert!(matches!(parse_specs(&p), Err(Error::InvalidIndexSpec(_))));
        let s = IndexSpec::new("a", "vec-hnsw", &["v"]);
        p.insert(
            INDEXES_PROPERTY.into(),
            write_specs(&[s.clone(), s.clone()]),
        );
        assert!(matches!(parse_specs(&p), Err(Error::InvalidIndexSpec(_))));
        p.insert(
            INDEXES_PROPERTY.into(),
            write_specs(std::slice::from_ref(&s)),
        );
        assert_eq!(parse_specs(&p).unwrap(), vec![s]);
        p.insert(
            INDEXES_PROPERTY.into(),
            r#"[{"name":"a","kind":"x","columns":[],"extra":1}]"#.into(),
        );
        assert!(parse_specs(&p).is_err(), "unknown fields are refused");
    }
}
