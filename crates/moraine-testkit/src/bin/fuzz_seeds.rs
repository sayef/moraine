//! Writes the seed corpora of the cargo-fuzz targets (`fuzz/`, T13) from the checked-in golden
//! files: `<dir>/<target>/<name>`, one seed per file, in the input format each target reads.
//! `fuzz/run.sh` runs this when `fuzz/seeds/` is missing.
//!
//!     cargo run -p moraine-testkit --bin fuzz_seeds -- fuzz/seeds

use std::collections::BTreeMap;
use std::path::Path;

use moraine_testkit::golden;
use serde_json::Value;

/// The property keys the `table_properties` target selects with its first byte, in its order.
const PROPERTY_KEYS: &[&str] = &[
    "moraine.formatVersion",
    "moraine.primaryKey",
    "moraine.enforcePrimaryKey",
    "moraine.clusterBy",
    "moraine.indexes",
    "moraine.gracePeriodSeconds",
    "moraine.pageRowLimit",
    "moraine.rowGroupRows",
    "moraine.targetFileBytes",
    "moraine.pins",
    "delta.checkpointInterval",
];

struct Seeds<'a> {
    dir: &'a Path,
    count: usize,
}

impl Seeds<'_> {
    fn put(&mut self, target: &str, name: &str, bytes: &[u8]) {
        let d = self.dir.join(target);
        std::fs::create_dir_all(&d).expect("seed directory");
        let name: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        std::fs::write(d.join(name), bytes).expect("write seed");
        self.count += 1;
    }
}

/// Every action line of the golden table's log entries and pin manifests.
fn actions(files: &[(String, Vec<u8>)]) -> Vec<Value> {
    files
        .iter()
        .filter(|(n, _)| {
            n.ends_with(".json") && (n.contains("/_delta_log/") || n.contains("/meta/pins/"))
        })
        .flat_map(|(_, b)| {
            String::from_utf8_lossy(b)
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn main() {
    let out = std::env::args()
        .nth(1)
        .expect("usage: fuzz_seeds <directory>");
    let out = Path::new(&out);
    let mut s = Seeds { dir: out, count: 0 };
    let files = golden::table_files();

    // Bundles: the golden sidecars of every kind, and the one inside the golden table.
    for e in std::fs::read_dir(golden::dir()).expect("golden directory") {
        let p = e.expect("entry").path();
        if p.extension().is_some_and(|x| x == "osix") {
            let name = p.file_name().unwrap().to_string_lossy().into_owned();
            s.put("bundle", &name, &std::fs::read(&p).expect("bundle"));
        }
    }
    for (n, b) in &files {
        let name = n.rsplit('/').next().unwrap_or(n);
        if n.ends_with(".osix") {
            s.put("bundle", &format!("table-{name}"), b);
        }
        if n.ends_with(".checkpoint.parquet") || n.ends_with("/_last_checkpoint") {
            s.put("checkpoint", name, b);
        }
        if n.contains("/meta/pins/") {
            s.put("pin_manifest", name, b);
        }
        if n.contains("/meta/leases/") {
            s.put("lease", name, &[[0u8].as_slice(), b].concat());
        }
        if n.contains("/meta/inbox/") {
            s.put("inbox", name, b);
        }
    }

    // Log entries: each table's whole log (entries separated by NUL), and each entry alone.
    for table in ["t", "v"] {
        let prefix = format!("{}/{table}/data/_delta_log/", golden::TABLE_DB);
        let entries: Vec<&(String, Vec<u8>)> = files
            .iter()
            .filter(|(n, _)| n.starts_with(&prefix) && n.ends_with(".json"))
            .collect();
        let joined: Vec<u8> = entries
            .iter()
            .map(|(_, b)| b.as_slice())
            .collect::<Vec<_>>()
            .join(&0u8);
        s.put("log_entries", &format!("{table}-all"), &joined);
        for (n, b) in entries {
            let name = n.rsplit('/').next().unwrap_or(n);
            s.put("log_entries", &format!("{table}-{name}"), b);
        }
    }

    // Table properties, index specs and file statistics, from the actions.
    let mut stats_seen = 0;
    for (i, a) in actions(&files).iter().enumerate() {
        if let Some(conf) = a.pointer("/metaData/configuration") {
            let map: BTreeMap<String, String> =
                serde_json::from_value(conf.clone()).unwrap_or_default();
            s.put(
                "table_properties",
                &format!("map-{i}"),
                conf.to_string().as_bytes(),
            );
            for (k, key) in PROPERTY_KEYS.iter().enumerate() {
                if let Some(v) = map.get(*key) {
                    s.put(
                        "table_properties",
                        &format!("{key}-{i}"),
                        &[[k as u8].as_slice(), v.as_bytes()].concat(),
                    );
                }
            }
            if let Some(v) = map.get("moraine.indexes") {
                s.put("index_specs", &format!("table-{i}"), v.as_bytes());
            }
        }
        if let Some(st) = a.pointer("/add/stats").and_then(Value::as_str)
            && stats_seen < 16
        {
            stats_seen += 1;
            s.put(
                "file_stats",
                &format!("stats-{i}"),
                &[[1u8].as_slice(), st.as_bytes()].concat(),
            );
        }
    }
    let mut specs: Vec<moraine::IndexSpec> = golden::kinds().into_iter().map(|(_, s)| s).collect();
    specs.push(golden::fts().1);
    for (i, spec) in specs.iter().enumerate() {
        let v = serde_json::to_string(&vec![spec]).expect("spec JSON");
        s.put("index_specs", &format!("golden-{i}"), v.as_bytes());
    }

    // Full-text queries: the golden ones under each parameter set.
    for (i, q) in golden::TEXT_QUERIES.iter().enumerate() {
        for p in 0..4u8 {
            s.put(
                "text_query",
                &format!("q{i}-p{p}"),
                &[[p].as_slice(), q.as_bytes()].concat(),
            );
        }
    }
    println!("wrote {} seeds under {}", s.count, out.display());
}
