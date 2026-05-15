import Foundation
import swift_leveldb

public protocol LevelDBCodec: Sendable {
    associatedtype Value: Sendable

    func encode(_ value: Value) throws -> Data
    func decode(_ data: Data) throws -> Value
}

public enum LevelDBTypedError: Error, Equatable, CustomStringConvertible {
    case invalidUTF8

    public var description: String {
        switch self {
        case .invalidUTF8:
            "Stored bytes are not valid UTF-8."
        }
    }
}

public struct DataCodec: LevelDBCodec {
    public init() {}

    public func encode(_ value: Data) -> Data {
        value
    }

    public func decode(_ data: Data) -> Data {
        data
    }
}

public struct StringCodec: LevelDBCodec {
    public init() {}

    public func encode(_ value: String) -> Data {
        Data(value.utf8)
    }

    public func decode(_ data: Data) throws -> String {
        guard let string = String(data: data, encoding: .utf8) else {
            throw LevelDBTypedError.invalidUTF8
        }

        return string
    }
}

public struct JSONCodec<Value: Codable & Sendable>: LevelDBCodec {
    public init() {}

    public func encode(_ value: Value) throws -> Data {
        try JSONEncoder().encode(value)
    }

    public func decode(_ data: Data) throws -> Value {
        try JSONDecoder().decode(Value.self, from: data)
    }
}

public struct LevelDBTypedWriteBatch<KeyCodec: LevelDBCodec, ValueCodec: LevelDBCodec> {
    public typealias Key = KeyCodec.Value
    public typealias Value = ValueCodec.Value

    let rawBatch: WriteBatch
    private let keyCodec: KeyCodec
    private let valueCodec: ValueCodec

    public init(keyCodec: KeyCodec, valueCodec: ValueCodec) {
        rawBatch = WriteBatch()
        self.keyCodec = keyCodec
        self.valueCodec = valueCodec
    }

    public func put(_ value: Value, forKey key: Key) throws {
        let encodedKey = try keyCodec.encode(key)
        let encodedValue = try valueCodec.encode(value)
        rawBatch.put(encodedValue, forKey: encodedKey)
    }

    public func deleteValue(forKey key: Key) throws {
        let encodedKey = try keyCodec.encode(key)
        rawBatch.deleteValue(forKey: encodedKey)
    }

    public func putRaw(_ value: Data, forEncodedKey key: Data) {
        rawBatch.put(value, forKey: key)
    }

    public func deleteRawValue(forEncodedKey key: Data) {
        rawBatch.deleteValue(forKey: key)
    }
}

public actor LevelDBStore<KeyCodec: LevelDBCodec, ValueCodec: LevelDBCodec> {
    private let database: Database
    private let keyCodec: KeyCodec
    private let valueCodec: ValueCodec

    public init(
        path: String,
        keyCodec: KeyCodec,
        valueCodec: ValueCodec,
        openOptions: Database.OpenOptions = .default
    ) throws {
        database = try Database(path: path, options: openOptions)
        self.keyCodec = keyCodec
        self.valueCodec = valueCodec
    }

    public func put(
        _ value: ValueCodec.Value,
        forKey key: KeyCodec.Value,
        writeOptions: Database.WriteOptions = .default
    ) async throws {
        let encodedKey = try keyCodec.encode(key)
        let encodedValue = try valueCodec.encode(value)
        try database.put(encodedValue, forKey: encodedKey, writeOptions: writeOptions)
    }

    public func value(
        forKey key: KeyCodec.Value,
        readOptions: Database.ReadOptions = .default
    ) async throws -> ValueCodec.Value? {
        let encodedKey = try keyCodec.encode(key)
        guard let data = try database.get(encodedKey, readOptions: readOptions) else {
            return nil
        }

        return try valueCodec.decode(data)
    }

    public func deleteValue(
        forKey key: KeyCodec.Value,
        writeOptions: Database.WriteOptions = .default
    ) async throws {
        let encodedKey = try keyCodec.encode(key)
        try database.deleteValue(forKey: encodedKey, writeOptions: writeOptions)
    }

    public func write(
        writeOptions: Database.WriteOptions = .default,
        _ build: (LevelDBTypedWriteBatch<KeyCodec, ValueCodec>) throws -> Void
    ) async throws {
        let batch = LevelDBTypedWriteBatch(
            keyCodec: keyCodec,
            valueCodec: valueCodec
        )
        try build(batch)
        try database.write(batch.rawBatch, writeOptions: writeOptions)
    }
}

public typealias StringLevelDBStore = LevelDBStore<StringCodec, StringCodec>
public typealias JSONLevelDBStore<Value: Codable & Sendable> = LevelDBStore<StringCodec, JSONCodec<Value>>

public enum LevelDBStores {
    public static func strings(
        path: String,
        openOptions: Database.OpenOptions = .default
    ) throws -> StringLevelDBStore {
        try StringLevelDBStore(
            path: path,
            keyCodec: StringCodec(),
            valueCodec: StringCodec(),
            openOptions: openOptions
        )
    }

    public static func json<Value: Codable & Sendable>(
        path: String,
        valueType: Value.Type = Value.self,
        openOptions: Database.OpenOptions = .default
    ) throws -> JSONLevelDBStore<Value> {
        try JSONLevelDBStore<Value>(
            path: path,
            keyCodec: StringCodec(),
            valueCodec: JSONCodec<Value>(),
            openOptions: openOptions
        )
    }
}
