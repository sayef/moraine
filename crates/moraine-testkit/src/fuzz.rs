//! Deterministic input mutation for the corruption fuzz tests (D29, case X3).
//!
//! Byte-level edits (bit flips, overwrites, insertions, deletions, truncation, duplicated
//! ranges) find what a torn or bit-rotted object does to a parser. JSON-aware edits keep the
//! syntax valid and change one value (wrong types, extreme numbers, empty and huge strings,
//! swapped subtrees), which reaches the code behind the syntax check; strings that hold JSON
//! themselves (a schema, statistics, a property list) are mutated inside as well. Every
//! mutation is a function of the seed, so a failure replays from the seed alone.

use serde_json::Value;

use crate::SimRng;

/// Values that tend to break assumptions: wrong types, signs, limits, and empty containers.
fn interesting(rng: &mut SimRng) -> Value {
    match rng.below(18) {
        0 => Value::Null,
        1 => Value::Bool(rng.chance(0.5)),
        2 => (-1i64).into(),
        3 => 0i64.into(),
        4 => i64::MAX.into(),
        5 => i64::MIN.into(),
        6 => u64::MAX.into(),
        7 => serde_json::Number::from_f64(1e308).map_or(Value::Null, Value::Number),
        8 => serde_json::Number::from_f64(-0.5).map_or(Value::Null, Value::Number),
        9 => Value::String(String::new()),
        10 => Value::String("x".repeat(rng.range(1, 100_000) as usize)),
        11 => Value::String("\u{0}\u{FFFF}\u{10FFFF}é".into()),
        12 => Value::String("-1".into()),
        13 => Value::String("18446744073709551616".into()),
        14 => Value::Array(Vec::new()),
        15 => Value::Object(serde_json::Map::new()),
        16 => Value::String("[\"a\",".into()),
        _ => Value::String("{\"type\":\"struct\",\"fields\":[]}".into()),
    }
}

fn count_nodes(v: &Value) -> usize {
    1 + match v {
        Value::Array(a) => a.iter().map(count_nodes).sum(),
        Value::Object(o) => o.values().map(count_nodes).sum(),
        _ => 0,
    }
}

fn nth_node<'a>(v: &'a mut Value, n: &mut usize) -> Option<&'a mut Value> {
    if *n == 0 {
        return Some(v);
    }
    *n -= 1;
    match v {
        Value::Array(a) => {
            for x in a.iter_mut() {
                if let Some(r) = nth_node(x, n) {
                    return Some(r);
                }
            }
            None
        }
        Value::Object(o) => {
            for x in o.values_mut() {
                if let Some(r) = nth_node(x, n) {
                    return Some(r);
                }
            }
            None
        }
        _ => None,
    }
}

/// The `n`th node of `v` in pre-order (`n` below [`count_nodes`]).
fn node_mut(v: &mut Value, mut n: usize) -> &mut Value {
    nth_node(v, &mut n).expect("index below the node count")
}

/// One JSON-aware edit of `v`: a random node is replaced, removed, duplicated, or (when it is a
/// string holding JSON) mutated inside.
pub fn mutate_json(rng: &mut SimRng, v: &mut Value) {
    let total = count_nodes(v);
    let i = rng.below(total as u64) as usize;
    let donor = if rng.chance(0.2) {
        Some(node_mut(v, rng.below(total as u64) as usize).clone())
    } else {
        None
    };
    let node = node_mut(v, i);
    if let Value::String(s) = node
        && rng.chance(0.6)
        && let Ok(mut inner) = serde_json::from_str::<Value>(s)
        && (inner.is_object() || inner.is_array())
    {
        mutate_json(rng, &mut inner);
        *s = inner.to_string();
        return;
    }
    match (rng.below(4), node) {
        (0, Value::Object(o)) if !o.is_empty() => {
            let k = o.keys().nth(rng.below(o.len() as u64) as usize).cloned();
            if let Some(k) = k {
                o.remove(&k);
            }
        }
        (1, Value::Array(a)) if !a.is_empty() => {
            let j = rng.below(a.len() as u64) as usize;
            if rng.chance(0.5) {
                a.remove(j);
            } else {
                let x = a[j].clone();
                a.push(x);
            }
        }
        (_, node) => *node = donor.unwrap_or_else(|| interesting(rng)),
    }
}

/// One to four byte-level edits.
pub fn mutate_bytes(rng: &mut SimRng, input: &[u8]) -> Vec<u8> {
    let mut b = input.to_vec();
    for _ in 0..rng.range(1, 4) {
        let len = b.len() as u64;
        match rng.below(7) {
            0 if len > 0 => {
                let i = rng.below(len) as usize;
                b[i] ^= 1 << rng.below(8);
            }
            1 if len > 0 => {
                let i = rng.below(len) as usize;
                b[i] = [0u8, 0xFF, b'"', b'{', b'}', b',', b'\n', b'9'][rng.below(8) as usize];
            }
            2 => {
                let i = rng.below(len + 1) as usize;
                let n = rng.range(1, 16) as usize;
                let junk: Vec<u8> = (0..n).map(|_| rng.next_u64() as u8).collect();
                b.splice(i..i, junk);
            }
            3 if len > 0 => {
                let i = rng.below(len) as usize;
                let n = (rng.range(1, 64) as usize).min(b.len() - i);
                b.drain(i..i + n);
            }
            4 => b.truncate(rng.below(len + 1) as usize),
            5 if len > 0 => {
                let i = rng.below(len) as usize;
                let n = (rng.range(1, 256) as usize).min(b.len() - i);
                let copy = b[i..i + n].to_vec();
                let at = rng.below(b.len() as u64 + 1) as usize;
                b.splice(at..at, copy);
            }
            _ if len > 0 => {
                // Overwrite eight bytes with a little-endian extreme, as length fields are.
                let i = rng.below(len) as usize;
                let v = [u64::MAX, 0, 1 << 31, (1 << 32) - 1][rng.below(4) as usize];
                for (k, byte) in v.to_le_bytes().iter().enumerate() {
                    if let Some(x) = b.get_mut(i + k) {
                        *x = *byte;
                    }
                }
            }
            _ => b.push(rng.next_u64() as u8),
        }
    }
    b
}

/// A mutation of newline-delimited JSON (a Delta log entry): usually one JSON-aware edit of one
/// line, sometimes byte-level edits of the whole text, a dropped line or a duplicated one.
pub fn mutate_json_lines(rng: &mut SimRng, input: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(input);
    let mut lines: Vec<String> = text.lines().map(str::to_owned).collect();
    if lines.is_empty() || rng.chance(0.25) {
        return mutate_bytes(rng, input);
    }
    let i = rng.below(lines.len() as u64) as usize;
    match rng.below(10) {
        0 => {
            lines.remove(i);
        }
        1 => {
            let l = lines[i].clone();
            lines.insert(rng.below(lines.len() as u64 + 1) as usize, l);
        }
        _ => match serde_json::from_str::<Value>(&lines[i]) {
            Ok(mut v) => {
                for _ in 0..rng.range(1, 3) {
                    mutate_json(rng, &mut v);
                }
                lines[i] = v.to_string();
            }
            Err(_) => return mutate_bytes(rng, input),
        },
    }
    let mut out = lines.join("\n");
    out.push('\n');
    out.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutations_are_deterministic_and_change_input() {
        let input = br#"{"add":{"path":"a","size":3,"stats":"{\"numRecords\":1}"}}
{"txn":{"appId":"w","version":1}}
"#;
        let run = |seed| {
            let mut rng = SimRng::new(seed);
            (0..50)
                .map(|_| mutate_json_lines(&mut rng, input))
                .collect::<Vec<_>>()
        };
        assert_eq!(run(5), run(5));
        assert!(run(5).iter().filter(|m| m.as_slice() != input).count() > 40);
    }
}
