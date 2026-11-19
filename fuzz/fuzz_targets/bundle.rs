//! Sidecar bundles (case X3): structure, section table and section checksums.
//! Half the inputs get their footer checksum recomputed, so mutations reach the checks behind it.
#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;
use moraine::fuzzing;

fuzz_target!(|data: &[u8]| {
    common::init();
    let _ = fuzzing::bundle(data);
    let mut sealed = data.to_vec();
    fuzzing::bundle_reseal(&mut sealed);
    if sealed != data {
        let _ = fuzzing::bundle(&sealed);
    }
});
