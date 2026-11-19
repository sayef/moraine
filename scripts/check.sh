#!/usr/bin/env bash
# The per-commit checks (docs/design/test-ids.md), run locally; .github/workflows/ci.yml
# runs the same checks. Formatting, clippy with warnings denied, and every test of the library
# and the test kit, including the toy simulation (10,000 seeds by default; MORAINE_SIM_SEEDS
# changes it).
#
#   scripts/check.sh            # everything
#   scripts/check.sh --quick    # 1,000 seeds
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "${1:-}" = "--quick" ]; then
  export MORAINE_SIM_SEEDS="${MORAINE_SIM_SEEDS:-1000}"
fi
cargo fmt --all -- --check
cargo fmt --manifest-path bench/Cargo.toml --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
# The library alone, with no features: the workspace lint above turns on the features the test
# kit asks for, so code that only compiles differently without them is linted here.
cargo clippy -p morainedb --all-targets -- -D warnings
cargo test -p morainedb -p moraine-testkit
# The aws-sdk feature: lint it, and run its unit tests (the profile test against the
# S3-compatible server runs only when that server's variables are set).
cargo clippy -p morainedb -p moraine-testkit --all-targets --features moraine-testkit/aws-sdk -- -D warnings
cargo test -p morainedb --features aws-sdk --lib store::aws
cargo test -p moraine-testkit --features aws-sdk --test aws_sdk
# The comparison harness (D22) is a workspace of its own, so the workspace lint above does not
# reach it; it builds kv0 with Lance and Delta Lake, which takes a while the first time. Run from
# bench/ so its .cargo/config.toml puts the build in the shared target directory.
(cd bench && cargo clippy --all-targets -- -D warnings)
