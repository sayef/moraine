# Contributing to moraine

Thanks for your interest in moraine. Bug reports, failing seeds, benchmarks, documentation fixes
and code are all welcome.

## Before you start

- For anything beyond a small fix, open an issue first so we can agree on the approach. moraine
  follows a written design; a change that departs from it needs an entry in
  [`DECISIONS.md`](DECISIONS.md) with the reason.
- Security issues go through [SECURITY.md](SECURITY.md), not public issues.
- Use synthetic data in tests, examples and issues.
- Unless you explicitly state otherwise, any contribution you intentionally submit for inclusion
  in moraine, as defined in the Apache-2.0 licence, is dual licensed under Apache-2.0 and MIT,
  without any additional terms or conditions.

## Development setup

You need Rust 1.95 or newer (the minimum supported version, set by delta-rs) and a C++ compiler
for the usearch index (the `vector` feature and the test kit). Docker is optional; it runs the
S3-compatible server for the backend tests.

```sh
git clone https://github.com/sayef/moraine && cd moraine
scripts/check.sh --quick     # format, clippy, tests and 1,000 simulation seeds
scripts/check.sh             # the same with 10,000 seeds, plus the aws-sdk feature and bench/
```

The workspace members are `crates/moraine` (the library, package `morainedb`),
`crates/moraine-testkit` (test tooling and the integration tests), `crates/moraine-cli` (empty
for now) and `spike/` (the spike's checks). `bench/` is a workspace of its own and builds
Lance through kv0; `scripts/check.sh` lints it, which takes a while the first time.

## Tests and the simulation

Most tests live in `crates/moraine-testkit/tests`. The simulation tests run many seeds of a
scenario against a fault-injecting store with a simulated clock and a seeded scheduler, and
check every outcome against a reference model. A failing seed is printed and replays exactly:

```sh
MORAINE_SIM_SEEDS=100000 cargo test --release -p moraine-testkit --test mutations_sim -- --nocapture
MORAINE_SIM_SEED=1234 cargo test -p moraine-testkit --test mutations_sim    # replay one seed
MORAINE_FUZZ_ITERS=20000 cargo test -p moraine-testkit --test fuzz -- --nocapture
```

Write the failing scenario first, then the fix. Never weaken an invariant check to make a seed
pass; add the seed to the scenario's regression list instead.

The backend tests run against any S3-compatible server. `spike/s3-server.sh` starts RustFS in
Docker:

```sh
spike/s3-server.sh start && source <(spike/s3-server.sh env)
cargo test -p moraine-testkit --test backends
spike/s3-server.sh stop
```

Tests that need a real AWS bucket are ignored by default; they write only under a random prefix
and delete it afterwards.

## Checks before a pull request

The pre-commit hooks run the same checks as CI, except the test suite:

```sh
pre-commit install           # or: prek install
pre-commit run --all-files
```

They run cargo fmt and clippy (warnings denied), typos, gitleaks, actionlint and zizmor, and
basic file hygiene. CI also runs the tests, a short simulation and `cargo deny`.

- Keep delta-rs types out of the public API and out of every module except
  `crates/moraine/src/log/delta.rs`.
- A change on the write or read path keeps its request budget: the request-budget tests count
  the exact requests of each operation.
- Performance changes come with numbers from `bench/` (see [`docs/BENCH.md`](docs/BENCH.md)).

## Documentation

The site is built with mkdocs-material:

```sh
uv run --with-requirements docs/requirements.txt mkdocs serve      # http://127.0.0.1:8000
uv run --with-requirements docs/requirements.txt mkdocs build --strict
```

Brand assets (the mark, wordmark, banners, social preview, web fonts and the docs colour tokens) are generated; edit
`assets/src/build.py` and run it again rather than editing the SVG files by hand:

```sh
uv run --no-project --with fonttools --with brotli --with uharfbuzz --with resvg-py python assets/src/build.py
```

The identity is described in [`brand/DESIGN.md`](brand/DESIGN.md).

The benchmark charts are generated from the raw results too. Add a new run to `bench/results/`
as a `YYYY-MM-DD-<name>.jsonl` file and run `bench/charts/build.py`: it picks the newest run of
each kind by file name, redraws the SVG files in `docs/assets/charts/` and rewrites the marked
chart and data regions of `docs/benchmarks.md`, `docs/index.md` and this repository's README.
The sentences around the charts are written by hand, so read them against the new numbers.

```sh
uv run --no-project --with fonttools --with brotli python bench/charts/build.py
```

## Commits and pull requests

- Small, focused commits with an imperative subject line ("Add the key index to `get`").
- Describe what changed and why in the pull request, and link the issue.
- Update [`CHANGELOG.md`](CHANGELOG.md) under `Unreleased` for anything a user of the crate
  would notice.

## Releasing

Releases are cut by [release-plz](https://release-plz.dev) (`.github/workflows/release-plz.yml`)
and stay off until the repository variable `RELEASE_ENABLED` is `true`. With it on, every push to
`main` updates a release pull request (version and `CHANGELOG.md`); merging that pull request
publishes `morainedb` to crates.io, tags `v<version>` and creates the GitHub release.

- **Betas.** The first versions are betas toward 0.1.0 (`0.1.0-beta.N`). From a pre-release,
  release-plz bumps the last number (`0.1.0-beta.1` to `0.1.0-beta.2`), and a version with a
  pre-release part becomes a GitHub pre-release.
- **0.1.0.** release-plz never leaves a pre-release on its own, so I set the version explicitly:
  a pull request that sets `version = "0.1.0"` in the workspace manifest and runs
  `cargo update -w`. Once it is merged, the next release pull request releases 0.1.0.
- The notes under `## [Unreleased]` in `CHANGELOG.md` become the release's entry: release-plz only
  adds the version heading.
