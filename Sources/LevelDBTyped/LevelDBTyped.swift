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

public struct LevelDBStoreOptions: Sendable {
    public static let `default` = LevelDBStoreOptions()

    public var createIfMissing: Bool
    public var errorIfExists: Bool
    public var paranoidChecks: Bool
    public var writeBufferSize: Int?
    public var maxOpenFiles: Int?
    public var blockSize: Int?
    public var blockRestartInterval: Int?
    public var maxFileSize: Int?
    public var compression: Database.OpenOptions.Compression?
    public var lruCacheCapacity: Int?
    public var bloomFilterBitsPerKey: Int?

    public init(
        createIfMissing: Bool = true,
        errorIfExists: Bool = false,
        paranoidChecks: Bool = false,
        writeBufferSize: Int? = nil,
        maxOpenFiles: Int? = nil,
        blockSize: Int? = nil,
        blockRestartInterval: Int? = nil,
        maxFileSize: Int? = nil,
        compression: Database.OpenOptions.Compression? = nil,
        lruCacheCapacity: Int? = nil,
        bloomFilterBitsPerKey: Int? = nil
    ) {
        self.createIfMissing = createIfMissing
        self.errorIfExists = errorIfExists
        self.paranoidChecks = paranoidChecks
        self.writeBufferSize = writeBufferSize
        self.maxOpenFiles = maxOpenFiles
        self.blockSize = blockSize
        self.blockRestartInterval = blockRestartInterval
        self.maxFileSize = maxFileSize
        self.compression = compression
        self.lruCacheCapacity = lruCacheCapacity
        self.bloomFilterBitsPerKey = bloomFilterBitsPerKey
    }

    func makeOpenOptions() -> Database.OpenOptions {
        Database.OpenOptions(
            createIfMissing: createIfMissing,
            errorIfExists: errorIfExists,
            paranoidChecks: paranoidChecks,
            writeBufferSize: writeBufferSize,
            maxOpenFiles: maxOpenFiles,
            blockSize: blockSize,
            blockRestartInterval: blockRestartInterval,
            maxFileSize: maxFileSize,
            compression: compression,
            cache: lruCacheCapacity.map(Database.Cache.lru(capacity:)),
            filterPolicy: bloomFilterBitsPerKey.map(Database.FilterPolicy.bloom(bitsPerKey:))
        )
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
        options: LevelDBStoreOptions = .default
    ) throws {
        try self.init(
            path: path,
            keyCodec: keyCodec,
            valueCodec: valueCodec,
            openOptions: options.makeOpenOptions()
        )
    }

    /// Advanced initializer that accepts low-level database options.
    ///
    /// Prefer the `options: LevelDBStoreOptions` initializer for normal typed
    /// usage. Low-level options can include custom comparators, custom filter
    /// policies, and environments whose semantics must remain compatible with
    /// the database for its full lifetime.
    public init(
        path: String,
        keyCodec: KeyCodec,
        valueCodec: ValueCodec,
        openOptions: Database.OpenOptions
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

    public func scan(
        readOptions: Database.ReadOptions = .default
    ) async throws -> [(key: KeyCodec.Value, value: ValueCodec.Value)] {
        try await scan(from: nil, to: nil, readOptions: readOptions)
    }

    public func scan(
        from lowerBound: KeyCodec.Value? = nil,
        to upperBound: KeyCodec.Value? = nil,
        includingUpperBound: Bool = false,
        readOptions: Database.ReadOptions = .default
    ) async throws -> [(key: KeyCodec.Value, value: ValueCodec.Value)] {
        let lowerKey = try lowerBound.map { try keyCodec.encode($0) }
        let upperKey = try upperBound.map { try keyCodec.encode($0) }
        return try await scanEncodedRange(
            from: lowerKey,
            to: upperKey,
            includingUpperBound: includingUpperBound,
            readOptions: readOptions
        )
    }

    public func reverseScan(
        from lowerBound: KeyCodec.Value? = nil,
        to upperBound: KeyCodec.Value? = nil,
        includingUpperBound: Bool = false,
        readOptions: Database.ReadOptions = .default
    ) async throws -> [(key: KeyCodec.Value, value: ValueCodec.Value)] {
        let lowerKey = try lowerBound.map { try keyCodec.encode($0) }
        let upperKey = try upperBound.map { try keyCodec.encode($0) }
        return try await reverseScanEncodedRange(
            from: lowerKey,
            to: upperKey,
            includingUpperBound: includingUpperBound,
            readOptions: readOptions
        )
    }

    public func scanEncodedPrefix(
        _ prefix: Data,
        readOptions: Database.ReadOptions = .default
    ) async throws -> [(key: KeyCodec.Value, value: ValueCodec.Value)] {
        let iterator = database.makeIterator(readOptions: readOptions)
        iterator.seek(prefix)

        var entries: [(key: KeyCodec.Value, value: ValueCodec.Value)] = []
        while iterator.isValid {
            guard let key = iterator.key, key.starts(with: prefix) else {
                break
            }
            let value = iterator.value!
            entries.append((try keyCodec.decode(key), try valueCodec.decode(value)))
            iterator.next()
        }

        try iterator.checkError()
        return entries
    }

    public func scanEncodedRange(
        from lowerBound: Data? = nil,
        to upperBound: Data? = nil,
        includingUpperBound: Bool = false,
        readOptions: Database.ReadOptions = .default
    ) async throws -> [(key: KeyCodec.Value, value: ValueCodec.Value)] {
        let iterator = database.makeIterator(readOptions: readOptions)
        if let lowerBound {
            iterator.seek(lowerBound)
        } else {
            iterator.seekToFirst()
        }

        var entries: [(key: KeyCodec.Value, value: ValueCodec.Value)] = []
        while iterator.isValid {
            let key = iterator.key!
            if let upperBound {
                let comparison = Self.compare(key, upperBound)
                if comparison > 0 || (!includingUpperBound && comparison == 0) {
                    break
                }
            }
            let value = iterator.value!
            entries.append((try keyCodec.decode(key), try valueCodec.decode(value)))
            iterator.next()
        }

        try iterator.checkError()
        return entries
    }

    public func reverseScanEncodedRange(
        from lowerBound: Data? = nil,
        to upperBound: Data? = nil,
        includingUpperBound: Bool = false,
        readOptions: Database.ReadOptions = .default
    ) async throws -> [(key: KeyCodec.Value, value: ValueCodec.Value)] {
        let iterator = database.makeIterator(readOptions: readOptions)
        if let upperBound {
            iterator.seek(upperBound)
            if iterator.isValid {
                let key = iterator.key!
                let comparison = Self.compare(key, upperBound)
                if comparison > 0 || (!includingUpperBound && comparison == 0) {
                    iterator.previous()
                }
            } else {
                iterator.seekToLast()
            }
        } else {
            iterator.seekToLast()
        }

        var entries: [(key: KeyCodec.Value, value: ValueCodec.Value)] = []
        while iterator.isValid {
            let key = iterator.key!
            if let lowerBound, Self.compare(key, lowerBound) < 0 {
                break
            }
            let value = iterator.value!
            entries.append((try keyCodec.decode(key), try valueCodec.decode(value)))
            iterator.previous()
        }

        try iterator.checkError()
        return entries
    }

    private static func compare(_ lhs: Data, _ rhs: Data) -> Int {
        if lhs == rhs { return 0 }
        return lhs.lexicographicallyPrecedes(rhs) ? -1 : 1
    }
}

public typealias StringLevelDBStore = LevelDBStore<StringCodec, StringCodec>
public typealias JSONLevelDBStore<Value: Codable & Sendable> = LevelDBStore<StringCodec, JSONCodec<Value>>

public enum LevelDBStores {
    public static func strings(
        path: String,
        options: LevelDBStoreOptions = .default
    ) throws -> StringLevelDBStore {
        try StringLevelDBStore(
            path: path,
            keyCodec: StringCodec(),
            valueCodec: StringCodec(),
            options: options
        )
    }

    public static func strings(
        path: String,
        openOptions: Database.OpenOptions
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
        options: LevelDBStoreOptions = .default
    ) throws -> JSONLevelDBStore<Value> {
        try JSONLevelDBStore<Value>(
            path: path,
            keyCodec: StringCodec(),
            valueCodec: JSONCodec<Value>(),
            options: options
        )
    }

    public static func json<Value: Codable & Sendable>(
        path: String,
        valueType: Value.Type = Value.self,
        openOptions: Database.OpenOptions
    ) throws -> JSONLevelDBStore<Value> {
        try JSONLevelDBStore<Value>(
            path: path,
            keyCodec: StringCodec(),
            valueCodec: JSONCodec<Value>(),
            openOptions: openOptions
        )
    }
}
