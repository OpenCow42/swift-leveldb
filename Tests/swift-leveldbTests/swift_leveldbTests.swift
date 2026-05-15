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

private func temporaryDatabaseDirectory() -> URL {
    FileManager.default.temporaryDirectory
        .appendingPathComponent("swift-leveldb-\(UUID().uuidString)")
}
