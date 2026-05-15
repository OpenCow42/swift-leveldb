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
