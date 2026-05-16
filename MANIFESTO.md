# swift-leveldb Manifesto

`swift-leveldb` exists to provide a modern Swift interface to
[google/leveldb](https://github.com/google/leveldb).

LevelDB is a proven ordered key-value store with a small, focused C++ API. This
project's purpose is not to replace LevelDB or reinterpret its storage model. It
is to make LevelDB feel natural, safe, and maintainable from Swift while staying
faithful to the behavior, performance expectations, and operational semantics of
the upstream project.

## Principles

- Wrap LevelDB with clear Swift APIs that use Swift naming, types, memory
  ownership, and error handling idioms.
- Preserve LevelDB's behavior. Swift conveniences must not hide important
  storage, ordering, durability, or consistency semantics.
- Keep the interop layer explicit. Boundaries between Swift and LevelDB should be
  easy to audit, test, and reason about.
- Prefer small, composable APIs over broad abstractions that obscure what LevelDB
  is doing.
- Treat safety as a feature: lifecycle management, pointer ownership, resource
  cleanup, and concurrency expectations should be intentional and tested.
- Keep performance honest. Swift ergonomics should not introduce surprising
  overhead in common database operations.
- Track upstream LevelDB deliberately, documenting compatibility expectations and
  any known divergence.

## Quality Bar

Every meaningful change should be tested. The project should maintain 100%
Swift library line coverage, with tests that exercise Swift-facing behavior and the
interop boundary where relevant.

`swift test` is the baseline check for all changes. A change is not ready until
the test suite passes and any new behavior is covered by meaningful tests.

## What Good Looks Like

Good contributions make LevelDB easier to use from Swift without making it
harder to understand LevelDB itself. The best APIs are Swifty at the call site,
plain at the boundary, documented where semantics matter, and backed by tests
that would catch regressions in both convenience behavior and low-level interop.
