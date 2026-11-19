//! Delta log entries (case X2): the input is entries 0, 1, ... separated by NUL bytes (never
//! valid inside a JSON entry), parsed and folded into a snapshot as opening a table does.
#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;
use moraine::fuzzing;

fuzz_target!(|data: &[u8]| {
    common::init();
    let bodies: Vec<&[u8]> = data.split(|b| *b == 0).take(64).collect();
    let _ = fuzzing::log_entries(&bodies);
});
