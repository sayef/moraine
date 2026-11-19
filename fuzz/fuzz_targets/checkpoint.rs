//! Classic single-file checkpoints written by moraine (D47), and `_last_checkpoint`.
#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;
use moraine::fuzzing;

fuzz_target!(|data: &[u8]| {
    common::init();
    let _ = fuzzing::checkpoint(data);
    let _ = fuzzing::last_checkpoint(data);
});
