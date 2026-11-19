//! Lease generation files (D50), `meta/leases/<job>/<generation>.json`: the object name and the
//! record. The first byte picks one of a few names, so name parsing is reached as well.
#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;
use moraine::fuzzing;

const NAMES: &[&str] = &[
    "db/t/meta/leases/maintain/00000000000000000001.json",
    "db/t/meta/leases/ingest/18446744073709551615.json",
    "db/t/meta/leases/x/0000000000000000001.json",
    "db/t/meta/leases/x/00000000000000000001.jsonx",
];

fuzz_target!(|data: &[u8]| {
    common::init();
    let Some((&n, body)) = data.split_first() else {
        return;
    };
    let _ = fuzzing::lease(NAMES[n as usize % NAMES.len()], body);
});
