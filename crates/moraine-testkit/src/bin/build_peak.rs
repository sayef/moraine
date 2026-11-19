//! One build in a process of its own, for the memory-bounded build test (D35):
//! opens a table, builds its indexes under a memory budget, and prints the process's peak
//! resident memory before and after. Native allocations (usearch's C++) are invisible to a
//! Rust allocator count, so the test reads the kernel's high-water mark instead.
//!
//!     build_peak <database directory> <table> <budget MiB>

use moraine::Database;
use moraine::index::BuildOptions;

/// Peak resident set size in bytes (`VmHWM`), on Linux.
fn hwm() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = s.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let [_, dir, table, budget] = args.as_slice() else {
        eprintln!("usage: build_peak <database directory> <table> <budget MiB>");
        std::process::exit(2);
    };
    let budget: usize = budget.parse::<usize>().expect("budget") << 20;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let out = rt.block_on(async {
        let db = Database::builder(dir)
            .build_memory(budget, budget)
            .file_cache_bytes(1 << 20)
            .open()
            .await
            .expect("open database");
        let t = db.open_table(table).await.expect("open table");
        let before = hwm();
        let r = t
            .build_indexes(BuildOptions::default())
            .await
            .expect("build");
        let after = hwm();
        serde_json::json!({
            "hwm_before": before,
            "hwm_after": after,
            "built": r.built,
            "kinds": t.indexes().expect("specs").iter().map(|s| s.kind.clone()).collect::<Vec<_>>(),
            "parts": r.parts,
            "peak_builder_bytes": r.peak_builder_bytes,
        })
    });
    println!("{out}");
}
