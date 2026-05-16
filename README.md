# swift-leveldb

[![Swift Package Index](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2FOpenCow42%2Fswift-leveldb%2Fbadge%3Ftype%3Dswift-versions)](https://swiftpackageindex.com/OpenCow42/swift-leveldb)

`swift-leveldb` is a Swift Package Manager wrapper around
[google/leveldb](https://github.com/google/leveldb). The project aims to provide
modern Swift APIs while preserving LevelDB's behavior, performance expectations,
and storage semantics.

## Vendored LevelDB

This package vendors Google LevelDB source code under `Vendor/leveldb` so users
can build the package with SwiftPM without installing a separate system LevelDB
library.

The current vendored upstream is:

- LevelDB version: `1.23`
- Upstream commit: `99b3c03b3284f5886f9ef9a4ef703d57373e61be`
- Upstream repository: <https://github.com/google/leveldb>

SwiftPM builds the vendored C++ implementation through a local `CLevelDB` target.
Swift code imports the `CLevelDB` Clang module, which intentionally exposes only
LevelDB's C API from `leveldb/c.h`.

The intended boundary is:

```text
Swift API
  -> CLevelDB Clang module
    -> leveldb/c.h
      -> vendored LevelDB C++ implementation
```

Direct Swift/C++ interop is not the foundation of this package. Keeping the
Swift boundary on the C API gives the wrapper a smaller, more stable interop
surface.

See [VENDORING.md](VENDORING.md) for the update procedure and local integration
files.

## Products

The package exposes three Swift products:

- `swift-leveldb`: the low-level Swift wrapper over LevelDB's C API. Use this
  when you want direct `Data` access and explicit read/write/open options.
- `swift-leveldb-typed`: an optional higher-level layer with typed codecs and an
  actor-based async store. Use this when you want Swift-native values and
  `async`/`await` ergonomics.
- `swift-leveldb-zstd`: an optional codec layer backed by
  [facebook/zstd](https://github.com/facebook/zstd). Use this when you want to
  compose Zstandard compression with another codec.

The typed and ZSTD products depend on the low-level product. They do not
introduce a second LevelDB build or a second vendored source tree.

The package also exposes benchmark executables:

- `swift-leveldb-bench`: a Swift benchmark runner modeled after LevelDB's
  `db_bench` scenarios.
- `leveldb-db-bench`: the vendored upstream C++ `db_bench` runner, useful as a
  native LevelDB comparison point.

For normal typed usage, prefer `LevelDBStoreOptions`. It exposes safe common
settings while omitting custom comparators, custom filter policies, and custom
environments. See [LOW_LEVEL_API_SAFETY.md](LOW_LEVEL_API_SAFETY.md) before
using those low-level escape hatches.

### Low-Level Usage

```swift
import Foundation
import swift_leveldb

let database = try Database(path: "/tmp/example.leveldb")
try database.put("value", forKey: "key")

let value = try database.string(forKey: "key")
```

### Typed Async Usage

```swift
import LevelDBTyped

struct User: Codable, Sendable {
    var id: Int
    var name: String
    var email: String
    var country: String
}

let users = try LevelDBStores.json(
    path: "/tmp/users.leveldb",
    valueType: User.self,
    options: LevelDBStoreOptions(bloomFilterBitsPerKey: 10)
)

try await users.put(User(id: 1, name: "Ada"), forKey: "users/1")
let user = try await users.value(forKey: "users/1")
```

`LevelDBTyped` uses codecs to convert Swift values to and from LevelDB bytes.
`Codable` JSON is provided as a default, but callers can define custom codecs for
other formats.

### ZSTD Codec Usage

```swift
import LevelDBTyped
import LevelDBZstd

struct User: Codable, Sendable {
    var id: Int
    var name: String
}

let users = try LevelDBStore(
    path: "/tmp/users.leveldb",
    keyCodec: StringCodec(),
    valueCodec: ZstdCodec(wrapping: JSONCodec<User>())
)

try await users.put(User(id: 1, name: "Ada"), forKey: "users/1")
let user = try await users.value(forKey: "users/1")
```

`ZstdCodec` wraps another codec. That keeps compression separate from
serialization, so the same wrapper can be used with JSON, Protobuf, raw `Data`,
or a custom binary codec.

### Atomic Records And Indexes

Use write batches to commit a compressed record and its index entries together:

```swift
try await users.write { batch in
    try batch.put(user, forKey: "record:user:\(user.id)")
    batch.putRaw(
        Data("\(user.id)".utf8),
        forEncodedKey: Data("index:user_by_email:\(user.email)".utf8)
    )
    batch.putRaw(
        Data(),
        forEncodedKey: Data("index:users_by_country:\(user.country):\(user.id)".utf8)
    )
}
```

The record value is encoded by `ZstdCodec(wrapping: JSONCodec<User>())`, while
the index keys stay plain and ordered for prefix/range scans.

## Development

Run the test suite with:

```sh
swift test
```

Every meaningful change should include meaningful tests. The project target is
100% Swift library line coverage. Check it with:

```sh
Scripts/check-coverage.sh
```

Run the Swift benchmark with:

```sh
swift run swift-leveldb-bench --benchmarks=fillseq,readrandom,seekrandom --num=10000 --reads=10000
```

Run the upstream LevelDB comparison benchmark with:

```sh
swift run leveldb-db-bench --benchmarks=fillseq,readrandom,seekrandom --num=10000 --reads=10000
```

## License

`swift-leveldb` is licensed under the BSD 3-Clause License. The vendored LevelDB
source under `Vendor/leveldb` is licensed separately by The LevelDB Authors under
the BSD 3-Clause License; see `Vendor/leveldb/LICENSE`.
