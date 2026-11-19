//! One writer process of the multi-process stress test (T8).
//!
//! The table holds `id` (unique per row), `process`, `kind` (`keep` or `junk`) and `n`. A
//! writer appends batches with a transaction id per batch (its run and sequence), deletes its
//! own junk rows, queues batches for the ingestor, and reads its own rows back; three of the
//! processes also run maintenance. Every acknowledged write is appended to the file named by
//! `MORAINE_STRESS_ACKS` before the next one starts, so a checker can tell what must be in the
//! table after any number of `kill -9`.

use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use moraine::expr::{col, lit};
use moraine::{CompactionPolicy, Error, MaintainOptions, Table};

/// The stress table's schema.
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("process", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("n", DataType::Int64, false),
    ]))
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn batch(process: &str, run: &str, seq: i64, rows: usize, kind: &str) -> RecordBatch {
    let ids: Vec<String> = (0..rows)
        .map(|i| format!("{process}.{run}.{seq}.{i}"))
        .collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(vec![process; rows])),
            Arc::new(StringArray::from(vec![kind; rows])),
            Arc::new(Int64Array::from(vec![seq; rows])),
        ],
    )
    .expect("stress batch")
}

/// Runs the writer for `duration`.
pub async fn run(
    t: &Table,
    process: &str,
    duration: Duration,
    maintain: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let acks_path = std::env::var("MORAINE_STRESS_ACKS")?;
    let mut acks = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&acks_path)?;
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos() as u64
        ^ u64::from(std::process::id());
    let mut rng = Rng(seed);
    let run = format!("{:08x}", rng.next() as u32);
    let app = format!("stress.{process}.{run}");
    let start = Instant::now();
    let mut seq: i64 = 0;
    let mut last_maintain = Instant::now();
    let mut ack = |line: String| -> std::io::Result<()> {
        writeln!(acks, "{line}")?;
        acks.sync_data()
    };
    while start.elapsed() < duration {
        let r: Result<(), Error> = async {
            match rng.below(10) {
                0..=4 => {
                    seq += 1;
                    let kind = if rng.below(4) == 0 { "junk" } else { "keep" };
                    let rows = 1 + rng.below(20) as usize;
                    let b = batch(process, &run, seq, rows, kind);
                    // Committed or already applied: either way the rows are in the table.
                    let _ = t.txn(app.as_str(), seq).append(vec![b]).await?;
                    ack(format!("append {app} {seq} {kind} {rows}"))?;
                }
                5 => {
                    t.delete(
                        col("process")
                            .eq(lit(process))
                            .and(col("kind").eq(lit("junk"))),
                    )
                    .await?;
                    ack(format!("delete {process}"))?;
                }
                6 | 7 => {
                    let rows = 1 + rng.below(10) as usize;
                    let qseq = rng.next() as i64 & 0xffff_ffff;
                    let b = batch(process, &format!("{run}q"), qseq, rows, "keep");
                    let s = t.submit(vec![b]).await?;
                    ack(format!(
                        "queued {run}q {qseq} {rows} {} {}",
                        s.submitter, s.sequence
                    ))?;
                }
                _ => {
                    let n = t
                        .scan()
                        .filter(col("process").eq(lit(process)))
                        .count()
                        .await?;
                    let _ = n;
                }
            }
            Ok(())
        }
        .await;
        match r {
            Ok(()) => {}
            // Conflicts and lost leases are expected under contention; anything else is not.
            Err(Error::TooManyConflicts { .. })
            | Err(Error::OutcomeUnknown { .. })
            | Err(Error::Throttled { .. })
            | Err(Error::CommitDeadlineExceeded { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        if maintain && last_maintain.elapsed() > Duration::from_secs(2) {
            let opts = MaintainOptions::default()
                .compaction(Some(
                    CompactionPolicy::default().min_file_age(Duration::from_secs(1)),
                ))
                .lease_ttl(Duration::from_secs(20))
                .checkpoint_interval(Some(10));
            match t.maintain(&opts).await {
                Ok(_) | Err(Error::TooManyConflicts { .. }) | Err(Error::OutcomeUnknown { .. }) => {
                }
                Err(e) => return Err(e.into()),
            }
            last_maintain = Instant::now();
        }
    }
    Ok(())
}
