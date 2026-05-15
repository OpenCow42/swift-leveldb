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
