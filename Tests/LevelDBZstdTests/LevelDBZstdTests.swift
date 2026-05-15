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

private func temporaryDatabaseDirectory() -> URL {
    FileManager.default.temporaryDirectory
        .appendingPathComponent("swift-leveldb-\(UUID().uuidString)")
}
