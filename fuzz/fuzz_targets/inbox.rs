//! Queued-write inbox objects (D51): one Arrow IPC stream per batch, decoded by the ingestor.
#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;
use moraine::fuzzing;

fuzz_target!(|data: &[u8]| {
    common::init();
    let _ = fuzzing::inbox(data);
});
