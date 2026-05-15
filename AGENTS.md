# AGENTS.md

This project is guided by [MANIFESTO.md](MANIFESTO.md). Read it before making
changes.

## Project Rules

- This repository is a Swift Package Manager project. Use `Package.swift` as the
  source of truth for targets, products, and dependencies.
- The goal is to wrap [google/leveldb](https://github.com/google/leveldb) with
  modern Swift interop while preserving LevelDB's semantics and performance
  expectations.
- Keep changes small, focused, and easy to review.
- Commit regularly in small units of work.
- Commit messages are expected to follow
  [Conventional Commits](https://www.conventionalcommits.org/), for example
  `feat: add database open options` or `test: cover iterator invalidation`.
- Maintain greater than 90% test coverage for the project.
- Each meaningful change must include meaningful tests.
- Run `swift test` before considering a change complete. Do not describe work as
  finished if the test suite has not passed, unless you clearly explain why it
  could not be run.

## Engineering Expectations

- Prefer Swift-native API design: clear names, value semantics where appropriate,
  explicit ownership, typed errors, and predictable resource management.
- Keep LevelDB interop boundaries visible and well tested.
- Avoid abstractions that conceal important LevelDB behavior such as ordering,
  snapshots, write options, read options, durability, and iterator validity.
- Document semantic decisions when Swift ergonomics require a choice that is not
  obvious from LevelDB's API.
- Do not make unrelated refactors while implementing a feature or fix.
- Preserve existing user work in the tree. Never revert changes you did not make
  unless explicitly asked.

## Verification Checklist

Before handing off work:

- Confirm the change matches the manifesto.
- Add or update tests for every meaningful behavior change.
- Run `swift test`.
- Check coverage and keep it above 90% when coverage tooling is available.
- Review the diff for accidental churn or unrelated edits.
