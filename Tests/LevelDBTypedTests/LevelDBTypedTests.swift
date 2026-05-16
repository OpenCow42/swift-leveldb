import Foundation
import LevelDBTyped
import Testing
import swift_leveldb

private struct User: Codable, Equatable, Sendable {
    var id: Int
    var name: String
}

@Test func storesStringValuesThroughActor() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let store = try LevelDBStores.strings(path: directory.path)

    try await store.put("value", forKey: "key")
    #expect(try await store.value(forKey: "key") == "value")

    try await store.put("updated", forKey: "key")
    #expect(try await store.value(forKey: "key") == "updated")

    try await store.deleteValue(forKey: "key")
    #expect(try await store.value(forKey: "key") == nil)
}

@Test func storesCodableValuesWithJSONCodec() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let store = try LevelDBStores.json(path: directory.path, valueType: User.self)
    let user = User(id: 42, name: "Ada")

    try await store.put(user, forKey: "users/42")

    #expect(try await store.value(forKey: "users/42") == user)
    #expect(try await store.value(forKey: "users/missing") == nil)
}

@Test func surfacesCodecDecodeFailures() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    do {
        let database = try Database(path: directory.path)
        try database.put(Data("not-json".utf8), forKey: Data("users/bad".utf8))
    }

    let store = try LevelDBStores.json(path: directory.path, valueType: User.self)

    do {
        _ = try await store.value(forKey: "users/bad")
        Issue.record("Expected JSON decoding to fail.")
    } catch {
        #expect(error is DecodingError)
    }
}

@Test func serializesConcurrentCallsThroughActor() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let store = try LevelDBStores.strings(path: directory.path)

    try await withThrowingTaskGroup(of: Void.self) { group in
        for index in 0..<20 {
            group.addTask {
                try await store.put("value-\(index)", forKey: "key-\(index)")
            }
        }

        try await group.waitForAll()
    }

    for index in 0..<20 {
        #expect(try await store.value(forKey: "key-\(index)") == "value-\(index)")
    }
}

@Test func scansOnlyKeysMatchingEncodedPrefix() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let store = try LevelDBStores.strings(path: directory.path)
    try await store.put("Ada", forKey: "index:user:name:ada")
    try await store.put("Grace", forKey: "index:user:name:grace")
    try await store.put("Paris", forKey: "index:city:name:paris")
    try await store.put("record", forKey: "record:user:1")

    let entries = try await store.scanEncodedPrefix(Data("index:user:name:".utf8))
    let keys = entries.map(\.key)
    let values = entries.map(\.value)

    #expect(keys == ["index:user:name:ada", "index:user:name:grace"])
    #expect(values == ["Ada", "Grace"])
}

@Test func scansBoundedRangesForwardAndReverse() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let store = try LevelDBStores.strings(path: directory.path)
    for key in ["a", "b", "c", "d"] {
        try await store.put(key.uppercased(), forKey: key)
    }

    let forward = try await store.scan(from: "b", to: "d")
    let reverse = try await store.reverseScan(from: "b", to: "d")

    #expect(forward.map(\.key) == ["b", "c"])
    #expect(reverse.map(\.key) == ["c", "b"])
}

@Test func opensStringStoreWithTypedDefaultOptions() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let store = try LevelDBStores.strings(path: directory.path, options: .default)

    try await store.put("value", forKey: "key")
    #expect(try await store.value(forKey: "key") == "value")
}

@Test func opensJSONStoreWithBloomFilterTypedOption() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let store = try LevelDBStores.json(
        path: directory.path,
        valueType: User.self,
        options: LevelDBStoreOptions(bloomFilterBitsPerKey: 10)
    )
    let user = User(id: 1, name: "Ada")

    try await store.put(user, forKey: "users/1")
    #expect(try await store.value(forKey: "users/1") == user)
}

@Test func opensTypedStoreWithLRUCacheOption() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let store = try LevelDBStores.strings(
        path: directory.path,
        options: LevelDBStoreOptions(lruCacheCapacity: 1024 * 1024)
    )

    try await store.put("cached", forKey: "key")
    #expect(try await store.value(forKey: "key") == "cached")
}

@Test func opensTypedStoreWithCompressionOptions() async throws {
    for compression in [Database.OpenOptions.Compression.none, .snappy] {
        let directory = temporaryDatabaseDirectory()
        defer {
            try? FileManager.default.removeItem(at: directory)
        }

        let store = try LevelDBStores.strings(
            path: directory.path,
            options: LevelDBStoreOptions(compression: compression)
        )

        try await store.put("compressed", forKey: "key")
        #expect(try await store.value(forKey: "key") == "compressed")
    }
}

@Test func preservesAdvancedOpenOptionsEscapeHatch() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let store = try LevelDBStores.strings(
        path: directory.path,
        openOptions: Database.OpenOptions(compression: Database.OpenOptions.Compression.none)
    )

    try await store.put("advanced", forKey: "key")
    #expect(try await store.value(forKey: "key") == "advanced")
}

private func temporaryDatabaseDirectory() -> URL {
    FileManager.default.temporaryDirectory
        .appendingPathComponent("swift-leveldb-\(UUID().uuidString)")
}
