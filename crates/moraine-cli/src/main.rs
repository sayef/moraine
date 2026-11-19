//! `moraine`: the operator tool for moraine databases.
//!
//! ```text
//! moraine [--option KEY=VALUE]... LOCATION COMMAND [ARGS]
//! ```
//!
//! `LOCATION` is a database URL (`s3://bucket/prefix`, `file:///path`) or a local directory.
//! Storage options (credentials, endpoint, region) come from `--option` and from the
//! environment, as `object_store` reads it (`AWS_*`, for example).

mod stress;

use std::collections::HashMap;
use std::process::ExitCode;
use std::time::Duration;

use moraine::{CompactionPolicy, Database, MaintainOptions, Table};

const USAGE: &str = "\
usage: moraine [--option KEY=VALUE]... LOCATION COMMAND [ARGS]

commands:
  tables                        list the tables
  inspect TABLE                 version, files, rows, levels, pins and indexes
  maintain TABLE [--no-leases] [--every SECONDS]
                                run maintenance passes (one, or one every SECONDS)
  compact TABLE [--full]        compact without a lease (--full: every run of small files)
  checkpoint TABLE              write a checkpoint at the newest version
  vacuum TABLE                  delete unreferenced files older than the grace period
  ingest TABLE                  commit queued writes from the inbox
  verify TABLE                  check data files and sidecars; exit status 1 on a problem
  repair TABLE                  remove references to missing data files
  pins TABLE                    list the pins
  pin TABLE NAME [VERSION]      pin a version (the newest by default)
  unpin TABLE NAME              remove a pin
  stress TABLE --seconds N --process ID [--maintain]
                                a writer of the multi-process stress test (T8)
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("moraine: {e}");
            return ExitCode::from(2);
        }
    };
    match rt.block_on(run(args)) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("moraine: {e}");
            ExitCode::from(1)
        }
    }
}

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn run(mut args: Vec<String>) -> Res<ExitCode> {
    let mut options = HashMap::new();
    while args.first().map(String::as_str) == Some("--option") {
        args.remove(0);
        let kv = take(&mut args, "KEY=VALUE")?;
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| format!("--option {kv}: expected KEY=VALUE"))?;
        options.insert(k.to_string(), v.to_string());
    }
    if args.is_empty() || args[0] == "--help" || args[0] == "-h" {
        print!("{USAGE}");
        return Ok(ExitCode::SUCCESS);
    }
    let location = location(&take(&mut args, "LOCATION")?)?;
    let command = take(&mut args, "COMMAND")?;
    let db = Database::builder(&location)
        .storage_options(options)
        .open()
        .await?;
    if command == "tables" {
        for t in db.table_names().await? {
            println!("{t}");
        }
        return Ok(ExitCode::SUCCESS);
    }
    let table = db.open_table(&take(&mut args, "TABLE")?).await?;
    match command.as_str() {
        "inspect" => inspect(&table).await?,
        "maintain" => {
            let leases = !flag(&mut args, "--no-leases");
            let every = value(&mut args, "--every")?
                .map(|s| s.parse::<u64>())
                .transpose()?;
            let opts = MaintainOptions::default().leases(leases);
            loop {
                let r = table.maintain(&opts).await?;
                println!("{r:#?}");
                match every {
                    Some(s) => tokio::time::sleep(Duration::from_secs(s)).await,
                    None => break,
                }
            }
        }
        "compact" => {
            let policy = if flag(&mut args, "--full") {
                CompactionPolicy::full()
            } else {
                CompactionPolicy::default()
            };
            println!("{:#?}", table.compact(&policy).await?);
        }
        "checkpoint" => match table.checkpoint().await? {
            Some(v) => println!("checkpoint written at version {v}"),
            None => println!("a checkpoint of version {} exists", table.version()),
        },
        "vacuum" => println!("{:#?}", table.vacuum().await?),
        "ingest" => println!("{:#?}", table.ingest(true).await?),
        "verify" => {
            let r = table.verify().await?;
            println!("{r:#?}");
            if !r.is_ok() {
                return Ok(ExitCode::from(1));
            }
        }
        "repair" => println!("{:#?}", table.repair().await?),
        "pins" => {
            for (name, v) in table.pins()? {
                println!("{name}\t{v}");
            }
        }
        "pin" => {
            let name = take(&mut args, "NAME")?;
            let version = match args.first() {
                Some(v) => v.parse()?,
                None => table.refresh().await?,
            };
            table.pin(&name, version).await?;
            println!("pinned {name} at version {version}");
        }
        "unpin" => {
            let name = take(&mut args, "NAME")?;
            table.unpin(&name).await?;
            println!("unpinned {name}");
        }
        "stress" => {
            let seconds: u64 = value(&mut args, "--seconds")?
                .ok_or("stress needs --seconds")?
                .parse()?;
            let process = value(&mut args, "--process")?.ok_or("stress needs --process")?;
            let maintain = flag(&mut args, "--maintain");
            stress::run(&table, &process, Duration::from_secs(seconds), maintain).await?;
        }
        other => return Err(format!("unknown command {other:?}\n\n{USAGE}").into()),
    }
    Ok(ExitCode::SUCCESS)
}

async fn inspect(t: &Table) -> Res<()> {
    let snap = t.snapshot().await?;
    let files = snap.files();
    println!("version\t{}", snap.version());
    println!("files\t{}", files.len());
    println!("rows\t{}", snap.num_rows());
    println!("bytes\t{}", files.iter().map(|f| f.size).sum::<u64>());
    let mut levels = std::collections::BTreeMap::new();
    for f in &files {
        *levels.entry(f.level).or_insert(0usize) += 1;
    }
    for (l, n) in levels {
        println!("level {l}\t{n} files");
    }
    for (name, v) in t.pins()? {
        println!("pin {name}\t{v}");
    }
    for i in t.indexes()? {
        println!("index {}\t{} on {:?}", i.name, i.kind, i.columns);
    }
    Ok(())
}

/// A URL, or a local directory turned into a `file://` URL.
fn location(s: &str) -> Res<String> {
    if s.contains("://") {
        return Ok(s.to_string());
    }
    let p = std::path::Path::new(s);
    std::fs::create_dir_all(p)?;
    let abs = std::fs::canonicalize(p)?;
    Ok(format!("file://{}", abs.display()))
}

fn take(args: &mut Vec<String>, what: &str) -> Res<String> {
    if args.is_empty() {
        return Err(format!("missing {what}\n\n{USAGE}").into());
    }
    Ok(args.remove(0))
}

fn flag(args: &mut Vec<String>, name: &str) -> bool {
    match args.iter().position(|a| a == name) {
        Some(i) => {
            args.remove(i);
            true
        }
        None => false,
    }
}

fn value(args: &mut Vec<String>, name: &str) -> Res<Option<String>> {
    match args.iter().position(|a| a == name) {
        Some(i) => {
            args.remove(i);
            if i >= args.len() {
                return Err(format!("{name} needs a value").into());
            }
            Ok(Some(args.remove(i)))
        }
        None => Ok(None),
    }
}
