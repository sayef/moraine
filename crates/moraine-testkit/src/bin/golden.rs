//! Regenerates the golden files of T12 (`crates/moraine-testkit/golden/`): the sidecar bundles
//! and their hits, and the golden table (`golden/table/`).
//!
//! Bumping the bundle format version or a kind's format version is deliberate: run this, keep
//! the previous files under a versioned name, and assert in `tests/golden.rs` that they are
//! rejected with `BundleError::Unsupported`.
//!
//!     cargo run -p moraine-testkit --bin golden

fn main() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("golden");
    std::fs::create_dir_all(&dir).expect("golden directory");
    for (name, bytes) in moraine_testkit::golden::all() {
        let path = dir.join(&name);
        std::fs::write(&path, bytes).expect("write golden file");
        println!("wrote {}", path.display());
    }
    // The golden table (every object kind a table holds): replaced as a whole, since object
    // names follow from the writes.
    let table = dir.join("table");
    if table.exists() {
        std::fs::remove_dir_all(&table).expect("remove the old golden table");
    }
    let (files, summary) = moraine_testkit::golden::table_with_summary();
    std::fs::write(dir.join("table.expected.json"), summary).expect("write the table summary");
    for (name, bytes) in files {
        let path = table.join(&name);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("golden table directory");
        std::fs::write(&path, bytes).expect("write golden table file");
    }
    println!("wrote {}", table.display());
}
