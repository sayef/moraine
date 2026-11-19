//! Child process for the multi-process spikes: appends `count` batches to one table.
//!
//! Usage: appender <backend spec> <table path> <writer id> <count> <rows per batch>

use moraine_spike::{Backend, run_appender};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(
        args.len(),
        6,
        "usage: appender <backend> <table> <writer> <count> <rows>"
    );
    let backend = Backend::from_spec(&args[1]);
    let writer: i64 = args[3].parse().unwrap();
    let count: i64 = args[4].parse().unwrap();
    let rows: usize = args[5].parse().unwrap();
    match run_appender(&backend, &args[2], writer, count, rows).await {
        Ok(stats) => println!("STATS {}", stats.to_line(writer)),
        Err(e) => {
            eprintln!("writer {writer} failed: {e}");
            std::process::exit(1);
        }
    }
}
