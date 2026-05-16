import Foundation
import Testing
@testable import swift_leveldb

@Test func exposesVendoredLevelDBVersion() {
    #expect(LevelDB.version == LevelDBVersion(major: 1, minor: 23))
}

@Test func storesReadsAndDeletesDataThroughCInterop() throws {
    let directory = FileManager.default.temporaryDirectory
        .appendingPathComponent("swift-leveldb-\(UUID().uuidString)")
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let database = try Database(path: directory.path)

    try database.put("value", forKey: "key")
    #expect(try database.string(forKey: "key") == "value")

    try database.deleteValue(forKey: "key")
    #expect(try database.string(forKey: "key") == nil)
}

@Test func appliesWriteBatchAtomically() throws {
    let directory = FileManager.default.temporaryDirectory
        .appendingPathComponent("swift-leveldb-\(UUID().uuidString)")
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let database = try Database(path: directory.path)

    try database.write { batch in
        batch.put("record", forKey: "record:user:123")
        batch.put("123", forKey: "index:user_by_email:ada@example.com")
        batch.put("", forKey: "index:users_by_country:FR:123")
    }

    #expect(try database.string(forKey: "record:user:123") == "record")
    #expect(try database.string(forKey: "index:user_by_email:ada@example.com") == "123")
    #expect(try database.string(forKey: "index:users_by_country:FR:123") == "")
}

@Test func appendingWriteBatchesAppliesBothSetsOfWrites() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)

    let first = WriteBatch()
    first.put("first", forKey: "a")
    first.put("replace-me", forKey: "b")

    let second = WriteBatch()
    second.put("second", forKey: "c")
    second.deleteValue(forKey: "b")

    first.append(second)
    try database.write(first)

    #expect(try database.string(forKey: "a") == "first")
    #expect(try database.string(forKey: "b") == nil)
    #expect(try database.string(forKey: "c") == "second")
}

@Test func writeBatchOperationsReturnsPutsAndDeletesInOrder() {
    let batch = WriteBatch()
    batch.put("one", forKey: "a")
    batch.deleteValue(forKey: "b")
    batch.put(Data([0x00, 0x01]), forKey: Data([0xff]))

    #expect(batch.operations() == [
        .put(key: Data("a".utf8), value: Data("one".utf8)),
        .delete(key: Data("b".utf8)),
        .put(key: Data([0xff]), value: Data([0x00, 0x01])),
    ])
}

@Test func iteratorScansForwardInBytewiseOrder() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)

    try database.put("2", forKey: "b")
    try database.put("1", forKey: "a")
    try database.put("3", forKey: "c")

    let iterator = database.makeIterator()
    iterator.seekToFirst()

    var keys: [String] = []
    while iterator.isValid {
        if let key = iterator.key {
            keys.append(String(decoding: key, as: UTF8.self))
        }
        iterator.next()
    }

    try iterator.checkError()
    #expect(keys == ["a", "b", "c"])
}

@Test func iteratorScansReverseInBytewiseOrder() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)

    try database.put("1", forKey: "a")
    try database.put("3", forKey: "c")
    try database.put("2", forKey: "b")

    let iterator = database.makeIterator()
    iterator.seekToLast()

    var keys: [String] = []
    while iterator.isValid {
        if let key = iterator.key {
            keys.append(String(decoding: key, as: UTF8.self))
        }
        iterator.previous()
    }

    try iterator.checkError()
    #expect(keys == ["c", "b", "a"])
}

@Test func iteratorSeekStartsAtFirstKeyGreaterThanOrEqualToTarget() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)

    try database.put("1", forKey: "a")
    try database.put("3", forKey: "c")
    try database.put("5", forKey: "e")

    let iterator = database.makeIterator()
    iterator.seek("b")

    #expect(iterator.key.map { String(decoding: $0, as: UTF8.self) } == "c")
    #expect(iterator.value.map { String(decoding: $0, as: UTF8.self) } == "3")
    try iterator.checkError()
}

@Test func iteratorCanCheckErrorAfterTraversal() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)

    try database.put("value", forKey: "key")

    let iterator = database.makeIterator()
    iterator.seekToFirst()
    while iterator.isValid {
        iterator.next()
    }

    try iterator.checkError()
    #expect(iterator.key == nil)
    #expect(iterator.value == nil)
}

@Test func snapshotReadSeesValueFromCreationTime() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)

    try database.put("old", forKey: "key")
    let snapshot = database.snapshot()
    try database.put("new", forKey: "key")

    let snapshotOptions = Database.ReadOptions(snapshot: snapshot)
    #expect(try database.string(forKey: "key", readOptions: snapshotOptions) == "old")
    #expect(try database.string(forKey: "key") == "new")
}

@Test func scopedSnapshotReadSeesValueFromCreationTime() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)

    try database.put("old", forKey: "key")
    let snapshotValue = try database.withSnapshot { snapshot in
        try database.put("new", forKey: "key")
        return try database.string(
            forKey: "key",
            readOptions: Database.ReadOptions(snapshot: snapshot)
        )
    }

    #expect(snapshotValue == "old")
    #expect(try database.string(forKey: "key") == "new")
}

@Test func releasedSnapshotDoesNotPreventFurtherReads() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)

    try database.put("value", forKey: "key")
    do {
        let snapshot = database.snapshot()
        let readOptions = Database.ReadOptions(snapshot: snapshot)
        #expect(try database.string(forKey: "key", readOptions: readOptions) == "value")
    }

    #expect(try database.string(forKey: "key") == "value")
}

private func temporaryDatabaseDirectory() -> URL {
    FileManager.default.temporaryDirectory
        .appendingPathComponent("swift-leveldb-\(UUID().uuidString)")
}

@Test func knownPropertyReturnsValueAndUnknownPropertyReturnsNil() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)

    let fileCount = database.property("leveldb.num-files-at-level0")
    #expect(fileCount.flatMap(Int.init) != nil)
    #expect(database.property("leveldb.not-a-real-property") == nil)
}

@Test func approximateSizesReturnValuesForSingleAndMultipleRangesIncludingEmptyKeys() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)

    try database.put(Data("empty-key-value".utf8), forKey: Data())
    try database.put("value-a", forKey: "a")
    try database.put("value-b", forKey: "b")

    let size = database.approximateSize(of: Database.KeyRange(start: Data(), limit: Data("z".utf8)))
    let sizes = database.approximateSizes(of: [
        Database.KeyRange(start: Data(), limit: Data("a".utf8)),
        Database.KeyRange(start: Data("a".utf8), limit: Data("z".utf8)),
    ])

    #expect(size >= 0)
    #expect(sizes.count == 2)
    #expect(sizes.allSatisfy { $0 >= 0 })
}

@Test func compactRangeCompletesWithoutDataLoss() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)

    try database.put("value-a", forKey: "a")
    try database.put("value-b", forKey: "b")
    try database.put(Data("empty-key-value".utf8), forKey: Data())

    database.compactRange(start: Data(), limit: Data("z".utf8))
    database.compactRange(start: Optional<Data>.none, limit: Optional<Data>.none)

    #expect(try database.string(forKey: "a") == "value-a")
    #expect(try database.string(forKey: "b") == "value-b")
    #expect(try database.get(Data()).map { String(decoding: $0, as: UTF8.self) } == "empty-key-value")
}

@Test func destroyRemovesDatabaseSoReopenWithoutCreateFails() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    var database: Database? = try Database(path: directory.path)
    try database?.put("value", forKey: "key")
    database = nil

    try Database.destroy(path: directory.path)

    do {
        _ = try Database(
            path: directory.path,
            options: Database.OpenOptions(createIfMissing: false)
        )
        Issue.record("Expected opening a destroyed database without createIfMissing to fail")
    } catch LevelDBError.openFailed {
        // Expected.
    }
}

@Test func repairCanRunOnExistingDatabasePath() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    var database: Database? = try Database(path: directory.path)
    try database?.put("value", forKey: "key")
    database = nil

    try Database.repair(path: directory.path)

    let repaired = try Database(
        path: directory.path,
        options: Database.OpenOptions(createIfMissing: false)
    )
    #expect(try repaired.string(forKey: "key") == "value")
}

@Test func openWithNoCompressionWritesAndReadsData() throws {
    try assertDatabaseWritesAndReads(
        options: Database.OpenOptions(compression: Database.OpenOptions.Compression.none)
    )
}

@Test func openWithSnappyCompressionWritesAndReadsData() throws {
    try assertDatabaseWritesAndReads(
        options: Database.OpenOptions(compression: .snappy)
    )
}

@Test func openWithLRUCacheWritesAndReadsData() throws {
    try assertDatabaseWritesAndReads(
        options: Database.OpenOptions(cache: .lru(capacity: 1024 * 1024))
    )
}

@Test func openWithBloomFilterPolicyWritesAndReadsData() throws {
    try assertDatabaseWritesAndReads(
        options: Database.OpenOptions(filterPolicy: .bloom(bitsPerKey: 10))
    )
}

@Test func openWithCustomFilterPolicyWritesAndReadsData() throws {
    let filterPolicy = Database.FilterPolicy.custom(
        name: "swift-leveldb.tests.always-match",
        createFilter: { keys in
            Data("keys:\(keys.count)".utf8)
        },
        keyMayMatch: { _, _ in true }
    )

    try assertDatabaseWritesAndReads(
        options: Database.OpenOptions(filterPolicy: filterPolicy)
    )
}

@Test func customFilterPolicyCallbackStateStaysAliveForDatabaseLifetime() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }

    final class CallbackState: @unchecked Sendable {
        var createdFilters = 0
        var matchChecks = 0
    }

    let state = CallbackState()
    let database: Database = try {
        let filterPolicy = Database.FilterPolicy.custom(
            name: "swift-leveldb.tests.stateful-always-match",
            createFilter: { keys in
                state.createdFilters += 1
                return Data("keys:\(keys.count)".utf8)
            },
            keyMayMatch: { _, _ in
                state.matchChecks += 1
                return true
            }
        )
        return try Database(
            path: directory.path,
            options: Database.OpenOptions(filterPolicy: filterPolicy)
        )
    }()

    try database.put("value", forKey: "key")
    database.compactRange()

    #expect(try database.string(forKey: "key") == "value")
    _ = try database.string(forKey: "missing")
    #expect(state.createdFilters > 0)
    #expect(state.matchChecks > 0)
}

@Test func customComparatorControlsIterationOrder() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(
        path: directory.path,
        options: Database.OpenOptions(comparator: .custom(name: "swift-leveldb.reverse-bytewise") { lhs, rhs in
            reverseBytewiseCompare(lhs, rhs)
        })
    )

    try database.put("2", forKey: "b")
    try database.put("1", forKey: "a")
    try database.put("3", forKey: "c")

    #expect(try collectKeys(database) == ["c", "b", "a"])
}

@Test func customComparatorCanReopenDatabaseWithSameComparator() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let comparator = Database.Comparator.custom(name: "swift-leveldb.reverse-bytewise") { lhs, rhs in
        reverseBytewiseCompare(lhs, rhs)
    }

    do {
        let database = try Database(
            path: directory.path,
            options: Database.OpenOptions(comparator: comparator)
        )
        try database.put("2", forKey: "b")
        try database.put("1", forKey: "a")
        try database.put("3", forKey: "c")
    }

    // LevelDB requires the same comparator name and ordering when reopening existing data;
    // changing comparator behavior for a populated database is unsafe and not enforced here.
    let reopened = try Database(
        path: directory.path,
        options: Database.OpenOptions(createIfMissing: false, comparator: comparator)
    )
    #expect(try collectKeys(reopened) == ["c", "b", "a"])
    #expect(try reopened.string(forKey: "b") == "2")
}

@Test func customComparatorCallbackStateStaysAliveForDatabaseLifetime() throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }

    let database: Database = try {
        let comparator = Database.Comparator.custom(name: "swift-leveldb.reverse-bytewise") { lhs, rhs in
            reverseBytewiseCompare(lhs, rhs)
        }
        return try Database(
            path: directory.path,
            options: Database.OpenOptions(comparator: comparator)
        )
    }()

    try database.put("2", forKey: "b")
    try database.put("1", forKey: "a")
    try database.put("3", forKey: "c")

    #expect(try collectKeys(database) == ["c", "b", "a"])
}

@Test func openOptionsResourcesCanBeSharedAcrossDatabaseLifetimes() throws {
    let firstDirectory = temporaryDatabaseDirectory()
    let secondDirectory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: firstDirectory)
        try? FileManager.default.removeItem(at: secondDirectory)
    }

    let cache = Database.Cache.lru(capacity: 1024 * 1024)
    let filterPolicy = Database.FilterPolicy.bloom(bitsPerKey: 10)
    let options = Database.OpenOptions(cache: cache, filterPolicy: filterPolicy)

    do {
        let database = try Database(path: firstDirectory.path, options: options)
        try database.put("first", forKey: "key")
        #expect(try database.string(forKey: "key") == "first")
    }

    let database = try Database(path: secondDirectory.path, options: options)
    try database.put("second", forKey: "key")
    #expect(try database.string(forKey: "key") == "second")
}

@Test func defaultEnvironmentCanOpenDatabase() throws {
    try assertDatabaseWritesAndReads(
        options: Database.OpenOptions(environment: .default)
    )
}

@Test func environmentTestDirectoryReturnsUsablePath() throws {
    let environment = Database.Environment.default
    let testDirectory = try #require(environment.testDirectory())
    let directory = URL(fileURLWithPath: testDirectory)
        .appendingPathComponent("swift-leveldb-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: directory) }

    let database = try Database(
        path: directory.path,
        options: Database.OpenOptions(environment: environment)
    )
    try database.put("value", forKey: "key")

    #expect(try database.string(forKey: "key") == "value")
}

private func assertDatabaseWritesAndReads(options: Database.OpenOptions) throws {
    let directory = temporaryDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }

    let database = try Database(path: directory.path, options: options)
    try database.put("value", forKey: "key")

    #expect(try database.string(forKey: "key") == "value")
}

private func collectKeys(_ database: Database) throws -> [String] {
    let iterator = database.makeIterator()
    iterator.seekToFirst()

    var keys: [String] = []
    while iterator.isValid {
        if let key = iterator.key {
            keys.append(String(decoding: key, as: UTF8.self))
        }
        iterator.next()
    }

    try iterator.checkError()
    return keys
}

private func reverseBytewiseCompare(_ lhs: Data, _ rhs: Data) -> ComparisonResult {
    if lhs == rhs {
        return .orderedSame
    }
    return rhs.lexicographicallyPrecedes(lhs) ? .orderedAscending : .orderedDescending
}
