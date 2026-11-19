# Security policy

## Supported versions

moraine is pre-1.0 and has no release yet. Security fixes go into the main branch and, once
releases exist, into the latest release.

## Reporting a vulnerability

Please do not open a public issue. Report vulnerabilities privately through
[GitHub security advisories](https://github.com/sayef/moraine/security/advisories/new).

Include what you found, how to reproduce it and what you think the impact is. I will acknowledge
the report within five working days and keep you informed until it is resolved. Reporters are
credited in the advisory unless they prefer otherwise.

## Scope

moraine reads and writes tables under an object-store prefix you configure, and trusts storage
that only authorised writers can modify. Credentials are passed to the object store as storage
options or taken from the environment (or, with the `aws-sdk` feature, from the AWS SDK's
credential chain); moraine does not log or persist them. In scope:

- crashes, out-of-bounds reads, or unbounded memory or time from log entries, table properties,
  Parquet files or sidecar bundles, including corrupted ones;
- a commit that applies twice, loses an acknowledged write, or makes a partial write visible;
- credential leaks.

Behaviour that requires write access to the bucket with intent to damage it (deleting objects,
for example) is out of scope: anyone who can write to the prefix can change the tables.
