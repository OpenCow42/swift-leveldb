# Low-Level API Safety

`swift-leveldb` exposes LevelDB's C API in a Swift-friendly form. Some low-level
features are powerful because LevelDB treats them as persistent database
contracts or invokes user callbacks from internal database work.

Most application code should prefer `LevelDBTyped.LevelDBStoreOptions`, which
intentionally omits custom comparators, custom filter policies, and custom
environments.

## Comparator Compatibility

Comparator names and ordering semantics are part of a LevelDB database's
compatibility contract.

Reopening an existing database with a comparator that has the same name but a
different ordering can make reads incorrect. The wrapper preserves LevelDB's API
shape, but it cannot prove semantic compatibility between two Swift comparator
closures.

Use custom comparators only when:

- the comparator name is stable and non-empty
- the ordering behavior is deterministic
- every future open of the same database uses identical ordering behavior

For most typed use cases, prefer designing ordered keys with the default bytewise
comparator instead of installing a custom comparator.

## Environment Identity

`Database.Environment` equality is identity-based. Two `.default` environment
wrappers may represent equivalent default LevelDB environments without comparing
equal.

Typed-store options do not expose environments. Use the low-level environment
API only when you need to pass a specific environment object through
`Database.OpenOptions`.

## Callback Thread Safety

Custom comparator and custom filter-policy callbacks may be invoked by LevelDB
while the database is in use.

Callbacks should:

- avoid unsynchronized shared mutable state
- be deterministic for the same inputs
- avoid blocking on unrelated long-running work
- keep captured state alive for the database lifetime

The wrapper retains callback state for the underlying LevelDB object, but it
cannot make captured application state thread-safe.

## Typed Safe Options

`LevelDBStoreOptions` is the safer typed-library corridor. It supports common
configuration:

- creation and validation flags
- buffer and file-size tuning
- compression selection
- LRU cache capacity
- Bloom filter bits per key

It deliberately does not expose:

- custom comparators
- custom filter policies
- environments

Advanced users can still use `Database.OpenOptions` directly through the
low-level API or the typed store's `openOptions:` initializer.
