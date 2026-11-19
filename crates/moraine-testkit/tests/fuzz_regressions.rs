//! Inputs the cargo-fuzz targets (`fuzz/`, T13) found crashing, kept in
//! `fuzz/regressions/<target>/` and run through the same entry point on every commit. Each must
//! now return (an error or an answer) without a panic.

use std::path::PathBuf;

use moraine::fuzzing;

fn inputs(target: &str) -> Vec<(String, Vec<u8>)> {
    let dir: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "..",
        "..",
        "fuzz",
        "regressions",
        target,
    ]
    .iter()
    .collect();
    let Ok(rd) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut v: Vec<(String, Vec<u8>)> = rd
        .map(|e| {
            let p = e.unwrap().path();
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read(&p).unwrap(),
            )
        })
        .collect();
    v.sort();
    v
}

/// A checkpoint whose embedded Arrow schema names a type arrow-ipc 59 does not implement made
/// the decoder panic; it is now `Error::Corrupt`.
#[test]
fn checkpoint_regressions_do_not_panic() {
    let v = inputs("checkpoint");
    assert!(!v.is_empty(), "no checkpoint regressions found");
    for (name, body) in v {
        let r = std::panic::catch_unwind(|| fuzzing::checkpoint(&body));
        assert!(r.is_ok(), "{name}: panicked");
        assert!(
            matches!(r.unwrap(), Err(moraine::Error::Corrupt { .. })),
            "{name}"
        );
    }
}

/// An inbox object whose message stated a body of 256 PiB made arrow's reader abort on the
/// allocation; the framing check now refuses it before the reader runs.
#[test]
fn inbox_regressions_do_not_abort() {
    let v = inputs("inbox");
    assert!(!v.is_empty(), "no inbox regressions found");
    for (name, body) in v {
        let r = std::panic::catch_unwind(|| fuzzing::inbox(&body));
        assert!(r.is_ok(), "{name}: panicked");
        assert!(r.unwrap().is_err(), "{name}");
    }
}
