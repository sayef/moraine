#!/usr/bin/env bash
# Builds and runs the cargo-fuzz targets (T13) on stable Rust: libFuzzer with the coverage
# instrumentation cargo-fuzz would add, without its address sanitizer (which needs nightly).
# `cargo fuzz run <target>` works too where cargo-fuzz and nightly are installed.
#
#   fuzz/run.sh <target> [seconds]       # default 120; seeds come from the golden files
#   fuzz/run.sh <target> --replay FILE   # run one input, for example a crash artifact
#   fuzz/run.sh --all [seconds]          # every target in turn
#   fuzz/run.sh --list
#
# The corpus grows in fuzz/corpus/<target>/ and crashing inputs land in
# fuzz/artifacts/<target>/ (both ignored by git). The build goes to $CARGO_TARGET_DIR/fuzz (or
# target/fuzz), apart from the normal build, since the instrumentation changes every crate.
set -euo pipefail
caller=$PWD
cd "$(dirname "$0")"
self="$PWD/$(basename "$0")"
root=$(cd .. && pwd)

TARGETS=(bundle log_entries checkpoint table_properties index_specs file_stats pin_manifest
  lease inbox text_query)

if [ "${1:-}" = "--list" ]; then
  printf '%s\n' "${TARGETS[@]}"
  exit 0
fi
if [ "${1:-}" = "--all" ]; then
  failed=()
  for t in "${TARGETS[@]}"; do
    "$self" "$t" "${2:-120}" || failed+=("$t")
  done
  if [ ${#failed[@]} -gt 0 ]; then
    echo "failed: ${failed[*]}" >&2
    exit 1
  fi
  exit 0
fi

target=${1:?usage: fuzz/run.sh <target> [seconds] | --replay FILE | --all | --list}
case " ${TARGETS[*]} " in
  *" $target "*) ;;
  *) echo "unknown target $target; fuzz/run.sh --list" >&2; exit 2 ;;
esac

main_target="${CARGO_TARGET_DIR:-$root/target}"
export CARGO_TARGET_DIR="$main_target/fuzz"
export RUSTFLAGS="${RUSTFLAGS:-} --cfg fuzzing -Cpasses=sancov-module \
-Cllvm-args=-sanitizer-coverage-level=4 \
-Cllvm-args=-sanitizer-coverage-inline-8bit-counters \
-Cllvm-args=-sanitizer-coverage-pc-table \
-Cllvm-args=-sanitizer-coverage-trace-compares"
# An explicit --target keeps the flags off build scripts and proc macros, as cargo-fuzz does.
host=$(rustc -vV | sed -n 's/^host: //p')
cargo build --release --target "$host" --bin "$target"
bin="$CARGO_TARGET_DIR/$host/release/$target"

if [ "${2:-}" = "--replay" ]; then
  input=${3:?a file to replay}
  case "$input" in /*) ;; *) input="$caller/$input" ;; esac
  exec "$bin" "$input"
fi

seconds=${2:-120}
corpus="corpus/$target"
artifacts="artifacts/$target"
seeds="seeds/$target"
mkdir -p "$corpus" "$artifacts"
# Seeds from the checked-in golden files, written by the test kit (fuzz/seeds is ignored).
if [ ! -d "$seeds" ]; then
  (cd "$root" && env -u RUSTFLAGS CARGO_TARGET_DIR="$main_target" \
    cargo run --quiet -p moraine-testkit --bin fuzz_seeds -- "$root/fuzz/seeds")
fi
# Inputs up to 1 MiB: the largest golden bundle is 415 KiB.
exec "$bin" -max_total_time="$seconds" -max_len=1048576 -rss_limit_mb=4096 -timeout=30 \
  -print_final_stats=1 -artifact_prefix="$artifacts/" "$corpus" "$seeds"
