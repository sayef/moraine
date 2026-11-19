# Versioning policy

moraine has two things that can break a user: the Rust API of the `morainedb` crate, and the
objects it leaves in a bucket. They are versioned separately, because a table written today has
to outlive the program that wrote it. This page states what each version number promises. The
on-disk formats themselves are described in [On-disk format](FORMAT.md).

## The crate API

The crate follows [Semantic Versioning](https://semver.org/), as Cargo reads it.

**Before 1.0** (0.x.y):

- A minor release (0.x → 0.x+1) may break the API. Every break is listed under "Changed" or
  "Removed" in the changelog, with what to write instead.
- A patch release (0.x.y → 0.x.y+1) does not break the API, and does not change any on-disk
  format in a way an older 0.x release cannot read.
- I deprecate before removing where it costs little (a renamed method keeps the old name for one
  minor release, marked `#[deprecated]`), but I do not promise it before 1.0.
- **Pre-releases of 0.1.0** (`0.1.0-beta.N`) may break the API between betas, without
  deprecations, and each break is listed in the changelog. Cargo picks a pre-release only for
  a version requirement that names one, so nobody gets one by accident. From 0.1.0 the
  rules above apply.

**From 1.0:**

- Breaking changes need a major release. Release automation runs `cargo-semver-checks`
  (`semver_check` in `release-plz.toml`), so an accidental break fails the release pull request.
- Public enums, the report structs moraine fills in and the option structs are
  `#[non_exhaustive]`; option structs are built from `Default` (or a named constructor such as
  `CompactionPolicy::full`) and setters. Adding a variant, an error case, a report field or an
  option is therefore a minor change, before 1.0 and after. A `match` on a moraine enum ends
  with a wildcard arm.
- Raising the minimum supported Rust version is a minor change, never a patch, and the
  changelog says so. The minimum is the `rust-version` in `Cargo.toml`, checked in CI.
- Raising a pinned dependency whose types appear in the API (Arrow's `RecordBatch` and schema
  types, DataFusion's `Expr` and `ScalarValue`, `object_store`'s `ObjectStore`, tantivy's
  `TextAnalyzer` behind `fts`, `serde_json::Value` in index specs) is a breaking change, since
  callers must move with it. Before 1.0 that happens in minor releases; after 1.0 I aim to keep
  these upgrades to major releases. Errors do not add to that list: `Error` holds the errors of
  those libraries as sources (`StorageError`, `LibraryError`), never as variant payloads, and
  only its `From` conversions name them.

**What is not covered:**

- Items marked `#[doc(hidden)]` (the `testing` and `fuzzing` modules), the `failpoints`
  feature, and everything in `moraine-testkit`, the CLI's output format and the benchmark
  harness.
- The extension traits under `moraine::index` (`IndexKind`, `FileIndex`, `IndexBuilder`,
  `ExactScorer`) and the bundle reader they use are public so applications can register their
  own index kinds. They follow the same rules, but before 1.0 they change more often than the
  table API.
- Performance, request counts and memory use. The request budgets are tested (D18), and a
  regression is a bug, not a breaking change.
- `Debug` output and error message text. Match on `Error` variants, not on strings.

## The on-disk format

The formats are versioned by what a reader needs to know, not by the crate version:

| Version | Where it is recorded | Current |
|---|---|---|
| Table format | Table property `moraine.formatVersion` | 1 |
| Delta protocol | The log's `protocol` action | reader 1, writer 2 |
| Bundle format | Bytes 4–5 of every `.osix` sidecar | 1 |
| Index kind format | Bytes 52–55 of every sidecar, per kind | 1 for each built-in kind |
| Index library format | Inside the spec hash (tantivy's index format, usearch's file version) | as pinned |

The rules:

1. **Every release reads every format an earlier release of the same major version wrote**, or
   rejects it with the documented error ([On-disk format](FORMAT.md#what-a-release-must-do)). The
   golden files in `crates/moraine-testkit/golden/` are the test: each release must pass
   `tests/golden.rs` and `tests/golden_table.rs` against the files checked in by earlier
   releases. A golden file is never regenerated to make a test pass; a deliberate format change
   adds new golden files next to the old ones.
2. **A release may write a new format only if older readers fail safely on it.** For the table
   format that means bumping `moraine.formatVersion` (older releases refuse the table with
   `Error::UnsupportedTableFeature` instead of misreading it). For sidecars it means bumping the
   bundle or kind format version, which also changes the spec hash: older readers ignore the new
   sidecars and answer through the exact route, newer readers ignore the old ones until
   maintenance rebuilds them. Sidecars are never needed for correct answers, so a sidecar format
   change is not a breaking change for users; a table format change is.
3. **Before 1.0**, a minor release may bump the table format version. It says so in the
   changelog, and the release that bumps it still reads the previous version (or ships a
   documented migration). A patch release never changes a format older releases cannot read.
4. **From 1.0**, the table format version changes only in a major release, and the next major
   release still reads tables of the previous one. Additive changes that older readers ignore
   safely (a new table property, a new file tag, a new object under `meta/`) are allowed in
   minor releases, and are listed in the changelog.
5. **Delta protocol.** moraine writes reader version 1 and writer version 2, and reads the
   features listed in [On-disk format](FORMAT.md#the-delta-table). Raising what it writes
   would lock out other Delta readers of the same tables, so it is treated like a table format
   change.
