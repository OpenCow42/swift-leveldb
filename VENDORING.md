# Vendoring LevelDB

This package vendors Google LevelDB source code so SwiftPM can build LevelDB
without requiring users to install a system library.

## Upstream

- Repository: <https://github.com/google/leveldb>
- Version: `1.23`
- Commit: `99b3c03b3284f5886f9ef9a4ef703d57373e61be`
- Vendored path: `Vendor/leveldb`

## Local Integration Files

The vendored source should stay as close to upstream as practical. Local SwiftPM
integration is intentionally small:

- `Vendor/leveldb/include/CLevelDB.h`
- `Vendor/leveldb/include/module.modulemap`
- `Package.swift` target configuration for `CLevelDB`

Swift imports the `CLevelDB` Clang module, which exposes only
`leveldb/c.h`. The LevelDB C++ implementation is compiled by SwiftPM, but Swift
code should continue to interact with LevelDB through the C API boundary.

## Update Procedure

When updating LevelDB:

1. Download or check out the desired upstream LevelDB tag.
2. Replace `Vendor/leveldb` with the upstream source for that tag.
3. Restore the local integration files listed above.
4. Update this file with the new upstream version and commit.
5. Review `Package.swift` source and exclude lists for upstream layout changes.
6. Run `swift test`.

If upstream changes the C API surface, update the Swift wrapper and tests in the
same change. Do not silently switch Swift to direct C++ interop without an
explicit project decision.
