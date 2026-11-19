//! Full-text query strings. moraine has no filter string parser (filters are
//! DataFusion expressions built in code), so the query text is the one string a caller passes
//! to a parser. The first byte picks the search parameters.
#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;
use moraine::fuzzing;
use serde_json::json;

fuzz_target!(|data: &[u8]| {
    common::init();
    let Some((&p, text)) = data.split_first() else {
        return;
    };
    let search = match p % 4 {
        0 => json!(null),
        1 => json!({"conjunction": true}),
        2 => json!({"boosts": {"title": 2.5}}),
        _ => json!({"conjunction": true, "boosts": {"body": 0.0, "title": -1.0}}),
    };
    let _ = fuzzing::text_query(&search, &String::from_utf8_lossy(text));
});
