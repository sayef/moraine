//! Pin manifests (D49), `meta/pins/<name>/<version>.json`: a log entry holding a version's
//! metadata and file list, read without the log.
#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;
use moraine::fuzzing;

fuzz_target!(|data: &[u8]| {
    common::init();
    let _ = fuzzing::pin_manifest(data);
});
