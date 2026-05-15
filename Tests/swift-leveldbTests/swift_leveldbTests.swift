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
