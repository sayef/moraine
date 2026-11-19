//! An `add` action's statistics JSON, parsed against a schema with a column of every type that
//! moraine prunes by. Malformed statistics are not an error; they must not panic.
#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;
use moraine::fuzzing;

fuzz_target!(|data: &[u8]| {
    common::init();
    let Some((&flags, rest)) = data.split_first() else {
        return;
    };
    let text = String::from_utf8_lossy(rest);
    let _ = fuzzing::file_stats(&text, &fuzzing::stats_schema(), flags & 1 == 1);
});
