import CLevelDB
import Foundation

public struct LevelDBVersion: Equatable, Sendable {
    public let major: Int
    public let minor: Int

    public init(major: Int, minor: Int) {
        self.major = major
        self.minor = minor
    }
}

public enum LevelDB {
    public static var version: LevelDBVersion {
        LevelDBVersion(
            major: Int(leveldb_major_version()),
            minor: Int(leveldb_minor_version())
        )
    }
}

public enum LevelDBError: Error, Equatable, CustomStringConvertible {
    case openFailed(String)
    case operationFailed(String)

    public var description: String {
        switch self {
        case .openFailed(let message), .operationFailed(let message):
            message
        }
    }
}

public final class WriteBatch {
    fileprivate let handle: OpaquePointer

    public init() {
        handle = leveldb_writebatch_create()
    }

    deinit {
        leveldb_writebatch_destroy(handle)
    }

    public func put(_ value: Data, forKey key: Data) {
        key.withLevelDBBytes { keyPointer, keyCount in
            value.withLevelDBBytes { valuePointer, valueCount in
                leveldb_writebatch_put(
                    handle,
                    keyPointer,
                    keyCount,
                    valuePointer,
                    valueCount
                )
            }
        }
    }

    public func put(_ value: String, forKey key: String) {
        put(Data(value.utf8), forKey: Data(key.utf8))
    }

    public func deleteValue(forKey key: Data) {
        key.withLevelDBBytes { keyPointer, keyCount in
            leveldb_writebatch_delete(handle, keyPointer, keyCount)
        }
    }

    public func deleteValue(forKey key: String) {
        deleteValue(forKey: Data(key.utf8))
    }

    public func clear() {
        leveldb_writebatch_clear(handle)
    }
}

public final class Database {
    public struct OpenOptions: Equatable, Sendable {
        public static let `default` = OpenOptions()

        public var createIfMissing: Bool
        public var errorIfExists: Bool
        public var paranoidChecks: Bool
        public var writeBufferSize: Int?
        public var maxOpenFiles: Int?
        public var blockSize: Int?
        public var blockRestartInterval: Int?
        public var maxFileSize: Int?

        public init(
            createIfMissing: Bool = true,
            errorIfExists: Bool = false,
            paranoidChecks: Bool = false,
            writeBufferSize: Int? = nil,
            maxOpenFiles: Int? = nil,
            blockSize: Int? = nil,
            blockRestartInterval: Int? = nil,
            maxFileSize: Int? = nil
        ) {
            self.createIfMissing = createIfMissing
            self.errorIfExists = errorIfExists
            self.paranoidChecks = paranoidChecks
            self.writeBufferSize = writeBufferSize
            self.maxOpenFiles = maxOpenFiles
            self.blockSize = blockSize
            self.blockRestartInterval = blockRestartInterval
            self.maxFileSize = maxFileSize
        }
    }

    public struct ReadOptions: Equatable, Sendable {
        public static let `default` = ReadOptions()

        public var verifyChecksums: Bool
        public var fillCache: Bool
        public var snapshot: Snapshot?

        public init(
            verifyChecksums: Bool = false,
            fillCache: Bool = true,
            snapshot: Snapshot? = nil
        ) {
            self.verifyChecksums = verifyChecksums
            self.fillCache = fillCache
            self.snapshot = snapshot
        }

        public static func == (lhs: ReadOptions, rhs: ReadOptions) -> Bool {
            lhs.verifyChecksums == rhs.verifyChecksums
                && lhs.fillCache == rhs.fillCache
                && lhs.snapshot === rhs.snapshot
        }
    }

    public struct WriteOptions: Equatable, Sendable {
        public static let `default` = WriteOptions()

        public var sync: Bool

        public init(sync: Bool = false) {
            self.sync = sync
        }
    }

    public final class Snapshot: @unchecked Sendable {
        private let database: Database
        fileprivate let handle: OpaquePointer

        fileprivate init(database: Database) {
            self.database = database
            handle = leveldb_create_snapshot(database.handle)
        }

        deinit {
            leveldb_release_snapshot(database.handle, handle)
        }
    }

    public final class Iterator {
        private let database: Database
        private let handle: OpaquePointer

        fileprivate init(database: Database, readOptions: ReadOptions) {
            self.database = database
            let rawOptions = Database.makeReadOptions(readOptions)
            defer { leveldb_readoptions_destroy(rawOptions) }
            handle = leveldb_create_iterator(database.handle, rawOptions)
        }

        deinit {
            leveldb_iter_destroy(handle)
        }

        public var isValid: Bool {
            leveldb_iter_valid(handle) != 0
        }

        public var key: Data? {
            guard isValid else { return nil }

            var keyCount = 0
            guard let key = leveldb_iter_key(handle, &keyCount) else {
                return nil
            }

            return Data(bytes: key, count: keyCount)
        }

        public var value: Data? {
            guard isValid else { return nil }

            var valueCount = 0
            guard let value = leveldb_iter_value(handle, &valueCount) else {
                return nil
            }

            return Data(bytes: value, count: valueCount)
        }

        public func seekToFirst() {
            leveldb_iter_seek_to_first(handle)
        }

        public func seekToLast() {
            leveldb_iter_seek_to_last(handle)
        }

        public func seek(_ key: Data) {
            key.withLevelDBBytes { keyPointer, keyCount in
                leveldb_iter_seek(handle, keyPointer, keyCount)
            }
        }

        public func seek(_ key: String) {
            seek(Data(key.utf8))
        }

        public func next() {
            leveldb_iter_next(handle)
        }

        public func previous() {
            leveldb_iter_prev(handle)
        }

        public func checkError() throws {
            var error: UnsafeMutablePointer<CChar>?
            leveldb_iter_get_error(handle, &error)

            if let error {
                throw LevelDBError.operationFailed(Database.consume(error))
            }
        }
    }

    private let handle: OpaquePointer

    public convenience init(path: String, createIfMissing: Bool = true) throws {
        try self.init(
            path: path,
            options: OpenOptions(createIfMissing: createIfMissing)
        )
    }

    public init(path: String, options: OpenOptions) throws {
        let rawOptions = leveldb_options_create()
        defer { leveldb_options_destroy(rawOptions) }

        leveldb_options_set_create_if_missing(rawOptions, options.createIfMissing.levelDBBool)
        leveldb_options_set_error_if_exists(rawOptions, options.errorIfExists.levelDBBool)
        leveldb_options_set_paranoid_checks(rawOptions, options.paranoidChecks.levelDBBool)
        if let writeBufferSize = options.writeBufferSize {
            leveldb_options_set_write_buffer_size(rawOptions, writeBufferSize)
        }
        if let maxOpenFiles = options.maxOpenFiles {
            leveldb_options_set_max_open_files(rawOptions, Int32(maxOpenFiles))
        }
        if let blockSize = options.blockSize {
            leveldb_options_set_block_size(rawOptions, blockSize)
        }
        if let blockRestartInterval = options.blockRestartInterval {
            leveldb_options_set_block_restart_interval(rawOptions, Int32(blockRestartInterval))
        }
        if let maxFileSize = options.maxFileSize {
            leveldb_options_set_max_file_size(rawOptions, maxFileSize)
        }

        var error: UnsafeMutablePointer<CChar>?
        let database = path.withCString { pathPointer in
            leveldb_open(rawOptions, pathPointer, &error)
        }

        if let error {
            throw LevelDBError.openFailed(Self.consume(error))
        }

        guard let database else {
            throw LevelDBError.openFailed("LevelDB did not return a database handle.")
        }

        handle = database
    }

    deinit {
        leveldb_close(handle)
    }

    public func write(_ batch: WriteBatch, writeOptions: WriteOptions = .default) throws {
        let rawOptions = leveldb_writeoptions_create()
        defer { leveldb_writeoptions_destroy(rawOptions) }

        leveldb_writeoptions_set_sync(rawOptions, writeOptions.sync.levelDBBool)

        var error: UnsafeMutablePointer<CChar>?
        leveldb_write(handle, rawOptions, batch.handle, &error)

        if let error {
            throw LevelDBError.operationFailed(Self.consume(error))
        }
    }

    public func write(
        writeOptions: WriteOptions = .default,
        _ build: (WriteBatch) throws -> Void
    ) throws {
        let batch = WriteBatch()
        try build(batch)
        try write(batch, writeOptions: writeOptions)
    }

    public func put(_ value: Data, forKey key: Data, sync: Bool = false) throws {
        try put(value, forKey: key, writeOptions: WriteOptions(sync: sync))
    }

    public func put(_ value: Data, forKey key: Data, writeOptions: WriteOptions) throws {
        let rawOptions = leveldb_writeoptions_create()
        defer { leveldb_writeoptions_destroy(rawOptions) }

        leveldb_writeoptions_set_sync(rawOptions, writeOptions.sync.levelDBBool)

        var error: UnsafeMutablePointer<CChar>?
        key.withLevelDBBytes { keyPointer, keyCount in
            value.withLevelDBBytes { valuePointer, valueCount in
                leveldb_put(
                    handle,
                    rawOptions,
                    keyPointer,
                    keyCount,
                    valuePointer,
                    valueCount,
                    &error
                )
            }
        }

        if let error {
            throw LevelDBError.operationFailed(Self.consume(error))
        }
    }

    public func put(_ value: String, forKey key: String, sync: Bool = false) throws {
        try put(Data(value.utf8), forKey: Data(key.utf8), sync: sync)
    }

    public func get(_ key: Data) throws -> Data? {
        try get(key, readOptions: .default)
    }

    public func get(_ key: Data, readOptions: ReadOptions) throws -> Data? {
        let rawOptions = Self.makeReadOptions(readOptions)
        defer { leveldb_readoptions_destroy(rawOptions) }

        var error: UnsafeMutablePointer<CChar>?
        var valueCount = 0
        let value = key.withLevelDBBytes { keyPointer, keyCount in
            leveldb_get(handle, rawOptions, keyPointer, keyCount, &valueCount, &error)
        }
        defer {
            if let value {
                leveldb_free(value)
            }
        }

        if let error {
            throw LevelDBError.operationFailed(Self.consume(error))
        }

        guard let value else {
            return nil
        }

        return Data(bytes: value, count: valueCount)
    }

    public func string(forKey key: String) throws -> String? {
        try string(forKey: key, readOptions: .default)
    }

    public func string(forKey key: String, readOptions: ReadOptions) throws -> String? {
        guard let data = try get(Data(key.utf8), readOptions: readOptions) else {
            return nil
        }

        return String(decoding: data, as: UTF8.self)
    }

    public func deleteValue(forKey key: Data, sync: Bool = false) throws {
        try deleteValue(forKey: key, writeOptions: WriteOptions(sync: sync))
    }

    public func deleteValue(forKey key: Data, writeOptions: WriteOptions) throws {
        let rawOptions = leveldb_writeoptions_create()
        defer { leveldb_writeoptions_destroy(rawOptions) }

        leveldb_writeoptions_set_sync(rawOptions, writeOptions.sync.levelDBBool)

        var error: UnsafeMutablePointer<CChar>?
        key.withLevelDBBytes { keyPointer, keyCount in
            leveldb_delete(handle, rawOptions, keyPointer, keyCount, &error)
        }

        if let error {
            throw LevelDBError.operationFailed(Self.consume(error))
        }
    }

    public func deleteValue(forKey key: String, sync: Bool = false) throws {
        try deleteValue(forKey: Data(key.utf8), sync: sync)
    }

    public func snapshot() -> Snapshot {
        Snapshot(database: self)
    }

    public func withSnapshot<Result>(_ body: (Snapshot) throws -> Result) rethrows -> Result {
        try body(snapshot())
    }

    public func iterator(readOptions: ReadOptions = .default) -> Iterator {
        Iterator(database: self, readOptions: readOptions)
    }

    public func makeIterator(readOptions: ReadOptions = .default) -> Iterator {
        iterator(readOptions: readOptions)
    }

    private static func makeReadOptions(_ options: ReadOptions) -> OpaquePointer {
        let rawOptions = leveldb_readoptions_create()!
        leveldb_readoptions_set_verify_checksums(
            rawOptions,
            options.verifyChecksums.levelDBBool
        )
        leveldb_readoptions_set_fill_cache(rawOptions, options.fillCache.levelDBBool)
        if let snapshot = options.snapshot {
            leveldb_readoptions_set_snapshot(rawOptions, snapshot.handle)
        }
        return rawOptions
    }

    private static func consume(_ error: UnsafeMutablePointer<CChar>) -> String {
        let message = String(cString: error)
        leveldb_free(error)
        return message
    }
}

private extension Bool {
    var levelDBBool: UInt8 {
        self ? 1 : 0
    }
}

private extension Data {
    func withLevelDBBytes<Result>(
        _ body: (UnsafePointer<CChar>, Int) throws -> Result
    ) rethrows -> Result {
        if isEmpty {
            var byte: UInt8 = 0
            return try withUnsafePointer(to: &byte) { pointer in
                try body(
                    UnsafeRawPointer(pointer).assumingMemoryBound(to: CChar.self),
                    0
                )
            }
        }

        return try withUnsafeBytes { buffer in
            let pointer = buffer.baseAddress!.assumingMemoryBound(to: CChar.self)
            return try body(pointer, buffer.count)
        }
    }
}
