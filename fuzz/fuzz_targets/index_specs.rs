//! Index specs (`moraine.indexes`): the JSON list, each kind's parameter
//! checks, and the canonical JSON and spec hash every sidecar is matched by.
#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;
use moraine::fuzzing;

fuzz_target!(|data: &[u8]| {
    common::init();
    let text = String::from_utf8_lossy(data);
    let _ = fuzzing::index_specs(&text);
});
