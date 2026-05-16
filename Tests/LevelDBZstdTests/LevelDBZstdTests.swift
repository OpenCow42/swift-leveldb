import Foundation
import LevelDBTyped
import LevelDBZstd
import Testing

private struct Event: Codable, Equatable, Sendable {
    var id: Int
    var payload: String
}

@Test func wrapsJSONCodecWithZstdCompression() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let store = try LevelDBStore(
        path: directory.path,
        keyCodec: StringCodec(),
        valueCodec: ZstdCodec(wrapping: JSONCodec<Event>())
    )

    let event = Event(
        id: 7,
        payload: String(repeating: "compressible ", count: 128)
    )

    try await store.put(event, forKey: "events/7")

    #expect(try await store.value(forKey: "events/7") == event)
}

@Test func wrapsStringCodecWithZstdCompression() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let store = try LevelDBStore(
        path: directory.path,
        keyCodec: StringCodec(),
        valueCodec: ZstdCodec(wrapping: StringCodec())
    )

    try await store.put(String(repeating: "hello ", count: 256), forKey: "message")

    #expect(try await store.value(forKey: "message") == String(repeating: "hello ", count: 256))
}

@Test func batchesCompressedRecordsWithPlainIndexKeys() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let store = try LevelDBStore(
        path: directory.path,
        keyCodec: StringCodec(),
        valueCodec: ZstdCodec(wrapping: JSONCodec<Event>())
    )

    let event = Event(
        id: 42,
        payload: String(repeating: "indexed ", count: 128)
    )

    try await store.write { batch in
        try batch.put(event, forKey: "record:event:42")
        batch.putRaw(Data("42".utf8), forEncodedKey: Data("index:event_by_id:42".utf8))
        batch.putRaw(Data(), forEncodedKey: Data("index:events_by_kind:test:42".utf8))
    }

    #expect(try await store.value(forKey: "record:event:42") == event)
}

@Test func adaptiveZstdCodecWorksInTypedStore() async throws {
    let directory = temporaryDatabaseDirectory()
    defer {
        try? FileManager.default.removeItem(at: directory)
    }

    let store = try LevelDBStore(
        path: directory.path,
        keyCodec: StringCodec(),
        valueCodec: ZstdCodec(
            wrapping: JSONCodec<Event>(),
            storageStrategy: .adaptive(minimumCompressionSavingsRatio: 0.10)
        )
    )

    let event = Event(
        id: 99,
        payload: String(repeating: "adaptive ", count: 256)
    )

    try await store.put(event, forKey: "events/99")

    #expect(try await store.value(forKey: "events/99") == event)
}

private func temporaryDatabaseDirectory() -> URL {
    FileManager.default.temporaryDirectory
        .appendingPathComponent("swift-leveldb-\(UUID().uuidString)")
}
