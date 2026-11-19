//! Table properties (the Delta `configuration` map): moraine's options, index specs and pins.
//! The input is either a JSON object of strings, or a selector byte and one property's value,
//! so the fuzzer reaches each value parser directly.
#![no_main]

use std::collections::BTreeMap;

use libfuzzer_sys::fuzz_target;

mod common;
use moraine::fuzzing;

const KEYS: &[&str] = &[
    "moraine.formatVersion",
    "moraine.primaryKey",
    "moraine.enforcePrimaryKey",
    "moraine.clusterBy",
    "moraine.indexes",
    "moraine.gracePeriodSeconds",
    "moraine.pageRowLimit",
    "moraine.rowGroupRows",
    "moraine.targetFileBytes",
    "moraine.pins",
    "delta.checkpointInterval",
];

fuzz_target!(|data: &[u8]| {
    common::init();
    if let Ok(map) = serde_json::from_slice::<BTreeMap<String, String>>(data) {
        let _ = fuzzing::table_properties(&map);
        return;
    }
    let Some((&k, value)) = data.split_first() else {
        return;
    };
    let key = KEYS[k as usize % KEYS.len()];
    let map = BTreeMap::from([(key.to_string(), String::from_utf8_lossy(value).into_owned())]);
    let _ = fuzzing::table_properties(&map);
    let _ = fuzzing::pins(&map);
});
