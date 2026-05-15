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

private func temporaryDatabaseDirectory() -> URL {
    FileManager.default.temporaryDirectory
        .appendingPathComponent("swift-leveldb-\(UUID().uuidString)")
}
