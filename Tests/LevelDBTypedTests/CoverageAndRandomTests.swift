import Foundation
import LevelDBTyped
import Testing
import swift_leveldb

private struct CoverageUser: Codable, Equatable, Sendable {
    var id: Int
    var name: String
}

@Test func dataCodecRoundTripsBytesAndStringCodecRejectsInvalidUTF8() throws {
    let bytes = Data([0x00, 0xff, 0x01])
    let dataCodec = DataCodec()
    #expect(dataCodec.encode(bytes) == bytes)
    #expect(dataCodec.decode(bytes) == bytes)
    #expect(LevelDBTypedError.invalidUTF8.description == "Stored bytes are not valid UTF-8.")

    do {
        _ = try StringCodec().decode(Data([0xff, 0xfe]))
        Issue.record("Expected invalid UTF-8 to fail")
    } catch LevelDBTypedError.invalidUTF8 {
        // Expected.
    }
}

@Test func typedBatchDeletesTypedAndRawKeys() async throws {
    let directory = temporaryTypedCoverageDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let store = try LevelDBStores.strings(path: directory.path)

    try await store.put("typed", forKey: "typed")
    try await store.put("raw", forKey: "raw")
    try await store.write { batch in
        try batch.deleteValue(forKey: "typed")
        batch.deleteRawValue(forEncodedKey: Data("raw".utf8))
    }

    #expect(try await store.value(forKey: "typed") == nil)
    #expect(try await store.value(forKey: "raw") == nil)
}

@Test func typedScansCoverUnboundedAndInclusiveRanges() async throws {
    let directory = temporaryTypedCoverageDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let store = try LevelDBStores.strings(path: directory.path)

    for key in ["a", "b", "c"] {
        try await store.put(key.uppercased(), forKey: key)
    }

    #expect(try await store.scan().map(\.key) == ["a", "b", "c"])
    #expect(try await store.scan(from: "a", to: "c", includingUpperBound: true).map(\.key) == ["a", "b", "c"])
    #expect(try await store.scanEncodedRange(from: nil, to: Data("b".utf8), includingUpperBound: true).map(\.key) == ["a", "b"])
    #expect(try await store.reverseScan().map(\.key) == ["c", "b", "a"])
    #expect(try await store.reverseScan(from: "a", to: nil, includingUpperBound: true).map(\.key) == ["c", "b", "a"])
    #expect(try await store.reverseScanEncodedRange(from: Data("b".utf8), to: Data("z".utf8)).map(\.key) == ["c", "b"])
}

@Test func jsonOpenOptionsOverloadAndTypedAllOptionsOpen() async throws {
    let firstDirectory = temporaryTypedCoverageDatabaseDirectory()
    let secondDirectory = temporaryTypedCoverageDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: firstDirectory)
        try? FileManager.default.removeItem(at: secondDirectory)
    }

    let jsonStore = try LevelDBStores.json(
        path: firstDirectory.path,
        valueType: CoverageUser.self,
        openOptions: Database.OpenOptions(paranoidChecks: true)
    )
    try await jsonStore.put(CoverageUser(id: 1, name: "Ada"), forKey: "1")
    #expect(try await jsonStore.value(forKey: "1") == CoverageUser(id: 1, name: "Ada"))

    let stringStore = try LevelDBStores.strings(
        path: secondDirectory.path,
        options: LevelDBStoreOptions(
            paranoidChecks: true,
            writeBufferSize: 64 * 1024,
            maxOpenFiles: 64,
            blockSize: 4096,
            blockRestartInterval: 8,
            maxFileSize: 256 * 1024,
            compression: Database.OpenOptions.Compression.none,
            lruCacheCapacity: 128 * 1024,
            bloomFilterBitsPerKey: 10
        )
    )
    try await stringStore.put("value", forKey: "key")
    #expect(try await stringStore.value(forKey: "key") == "value")
}

@Test func typedOptionsCanFailWhenDatabaseAlreadyExists() async throws {
    let directory = temporaryTypedCoverageDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }

    _ = try LevelDBStores.strings(path: directory.path)

    do {
        _ = try LevelDBStores.strings(
            path: directory.path,
            options: LevelDBStoreOptions(errorIfExists: true)
        )
        Issue.record("Expected opening with errorIfExists to fail")
    } catch LevelDBError.openFailed {
        // Expected.
    }
}

@Test func randomTypedOperationsMatchInMemoryOracle() async throws {
    let directory = temporaryTypedCoverageDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let store = try LevelDBStores.json(path: directory.path, valueType: CoverageUser.self)
    var random = TypedCoverageRandom(seed: 77)
    var oracle: [String: CoverageUser] = [:]

    for _ in 0..<100 {
        let id = random.uniform(40)
        let key = String(format: "user:%03d", id)
        if random.uniform(5) == 0 {
            try await store.deleteValue(forKey: key)
            oracle[key] = nil
        } else {
            let user = CoverageUser(id: id, name: "name-\(random.uniform(1_000))")
            try await store.put(user, forKey: key)
            oracle[key] = user
        }
    }

    for (key, value) in oracle {
        #expect(try await store.value(forKey: key) == value)
    }

    let scanned = try await store.scan()
    #expect(scanned.map(\.key) == oracle.keys.sorted())
}

private func temporaryTypedCoverageDatabaseDirectory() -> URL {
    FileManager.default.temporaryDirectory
        .appendingPathComponent("swift-leveldb-typed-coverage-\(UUID().uuidString)")
}

private struct TypedCoverageRandom {
    private var state: UInt64

    init(seed: UInt64) {
        state = seed
    }

    mutating func next() -> UInt64 {
        state = state &* 1_103_515_245 &+ 12_345
        return state
    }

    mutating func uniform(_ upperBound: Int) -> Int {
        Int(next() % UInt64(upperBound))
    }
}
