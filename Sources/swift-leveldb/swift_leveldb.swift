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
    public enum Operation: Equatable, Sendable {
        case put(key: Data, value: Data)
        case delete(key: Data)
    }

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

    public func append(_ batch: WriteBatch) {
        leveldb_writebatch_append(handle, batch.handle)
    }

    public func operations() -> [Operation] {
        let state = WriteBatchIterationState()
        let retainedState = Unmanaged.passRetained(state)
        defer { retainedState.release() }

        leveldb_writebatch_iterate(
            handle,
            retainedState.toOpaque(),
            { statePointer, keyPointer, keyCount, valuePointer, valueCount in
                let state = Unmanaged<WriteBatchIterationState>
                    .fromOpaque(statePointer!)
                    .takeUnretainedValue()
                state.operations.append(.put(
                    key: Data(bytes: keyPointer!, count: keyCount),
                    value: Data(bytes: valuePointer!, count: valueCount)
                ))
            },
            { statePointer, keyPointer, keyCount in
                let state = Unmanaged<WriteBatchIterationState>
                    .fromOpaque(statePointer!)
                    .takeUnretainedValue()
                state.operations.append(.delete(
                    key: Data(bytes: keyPointer!, count: keyCount)
                ))
            }
        )

        return state.operations
    }
}

private final class WriteBatchIterationState {
    var operations: [WriteBatch.Operation] = []
}

private final class ComparatorState {
    let compare: @Sendable (Data, Data) -> ComparisonResult
    let namePointer: UnsafeMutablePointer<CChar>
    let nameCount: Int

    init(name: String, compare: @escaping @Sendable (Data, Data) -> ComparisonResult) {
        self.compare = compare
        let nameBytes = name.utf8CString
        nameCount = nameBytes.count
        namePointer = UnsafeMutablePointer<CChar>.allocate(capacity: nameCount)
        nameBytes.withUnsafeBufferPointer { buffer in
            namePointer.initialize(from: buffer.baseAddress!, count: buffer.count)
        }
    }

    deinit {
        namePointer.deinitialize(count: nameCount)
        namePointer.deallocate()
    }
}

private final class FilterPolicyState {
    let createFilter: @Sendable ([Data]) -> Data
    let keyMayMatch: @Sendable (Data, Data) -> Bool
    let namePointer: UnsafeMutablePointer<CChar>
    let nameCount: Int

    init(
        name: String,
        createFilter: @escaping @Sendable ([Data]) -> Data,
        keyMayMatch: @escaping @Sendable (Data, Data) -> Bool
    ) {
        self.createFilter = createFilter
        self.keyMayMatch = keyMayMatch
        let nameBytes = name.utf8CString
        nameCount = nameBytes.count
        namePointer = UnsafeMutablePointer<CChar>.allocate(capacity: nameCount)
        nameBytes.withUnsafeBufferPointer { buffer in
            namePointer.initialize(from: buffer.baseAddress!, count: buffer.count)
        }
    }

    deinit {
        namePointer.deinitialize(count: nameCount)
        namePointer.deallocate()
    }
}

public final class Database {
    public struct OpenOptions: Equatable, Sendable {
        public enum Compression: Equatable, Sendable {
            case none
            case snappy
        }

        public static let `default` = OpenOptions()

        public var createIfMissing: Bool
        public var errorIfExists: Bool
        public var paranoidChecks: Bool
        public var writeBufferSize: Int?
        public var maxOpenFiles: Int?
        public var blockSize: Int?
        public var blockRestartInterval: Int?
        public var maxFileSize: Int?
        public var compression: Compression?
        public var cache: Cache?
        public var filterPolicy: FilterPolicy?
        public var comparator: Comparator?
        public var environment: Environment?

        public init(
            createIfMissing: Bool = true,
            errorIfExists: Bool = false,
            paranoidChecks: Bool = false,
            writeBufferSize: Int? = nil,
            maxOpenFiles: Int? = nil,
            blockSize: Int? = nil,
            blockRestartInterval: Int? = nil,
            maxFileSize: Int? = nil,
            compression: Compression? = nil,
            cache: Cache? = nil,
            filterPolicy: FilterPolicy? = nil,
            comparator: Comparator? = nil,
            environment: Environment? = nil
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
            self.cache = cache
            self.filterPolicy = filterPolicy
            self.comparator = comparator
            self.environment = environment
        }
    }

    /// A LevelDB environment used for filesystem, locking, and background work.
    ///
    /// This wrapper currently exposes only the default environment. Lower-level opaque C handle
    /// families such as `leveldb_logger_t`, `leveldb_filelock_t`, `leveldb_randomfile_t`,
    /// `leveldb_seqfile_t`, and `leveldb_writablefile_t` remain intentionally unwrapped because
    /// LevelDB's C API does not provide a complete public construction and lifecycle surface for
    /// them here; exposing unusable raw handles would not add a safe Swift API.
    public final class Environment: @unchecked Sendable, Equatable {
        fileprivate let handle: OpaquePointer

        private init(handle: OpaquePointer) {
            self.handle = handle
        }

        deinit {
            leveldb_env_destroy(handle)
        }

        public static var `default`: Environment {
            Environment(handle: leveldb_create_default_env()!)
        }

        public func testDirectory() -> String? {
            Self.stringAndFree(leveldb_env_get_test_directory(handle))
        }

        public static func == (lhs: Environment, rhs: Environment) -> Bool {
            lhs === rhs
        }

        private static func stringAndFree(_ value: UnsafeMutablePointer<CChar>?) -> String? {
            defer {
                if let value {
                    leveldb_free(value)
                }
            }

            guard let value else {
                return nil
            }

            return String(cString: value)
        }
    }

    public final class Comparator: @unchecked Sendable, Equatable {
        public let name: String
        fileprivate let handle: OpaquePointer

        private init(name: String, handle: OpaquePointer) {
            self.name = name
            self.handle = handle
        }

        deinit {
            leveldb_comparator_destroy(handle)
        }

        /// Creates a custom LevelDB comparator.
        ///
        /// The comparator name and ordering are persisted compatibility contract for a database.
        /// Reopen a database only with the same name and identical ordering behavior; LevelDB treats
        /// incompatible comparator behavior for existing data as unsafe.
        public static func custom(
            name: String,
            compare: @escaping @Sendable (Data, Data) -> ComparisonResult
        ) -> Comparator {
            let state = ComparatorState(name: name, compare: compare)
            let retainedState = Unmanaged.passRetained(state)
            let handle = leveldb_comparator_create(
                retainedState.toOpaque(),
                { statePointer in
                    Unmanaged<ComparatorState>.fromOpaque(statePointer!).release()
                },
                { statePointer, lhsPointer, lhsCount, rhsPointer, rhsCount in
                    let state = Unmanaged<ComparatorState>
                        .fromOpaque(statePointer!)
                        .takeUnretainedValue()
                    let lhs = Data(bytes: lhsPointer!, count: lhsCount)
                    let rhs = Data(bytes: rhsPointer!, count: rhsCount)

                    switch state.compare(lhs, rhs) {
                    case .orderedAscending:
                        return -1
                    case .orderedSame:
                        return 0
                    case .orderedDescending:
                        return 1
                    }
                },
                { statePointer in
                    let state = Unmanaged<ComparatorState>
                        .fromOpaque(statePointer!)
                        .takeUnretainedValue()
                    return UnsafePointer(state.namePointer)
                }
            )!

            return Comparator(name: name, handle: handle)
        }

        public static func == (lhs: Comparator, rhs: Comparator) -> Bool {
            lhs.name == rhs.name
        }
    }

    public final class Cache: @unchecked Sendable, Equatable {
        public enum Kind: Equatable, Sendable {
            case lru(capacity: Int)
        }

        public let kind: Kind
        fileprivate let handle: OpaquePointer

        private init(kind: Kind, handle: OpaquePointer) {
            self.kind = kind
            self.handle = handle
        }

        deinit {
            leveldb_cache_destroy(handle)
        }

        public static func lru(capacity: Int) -> Cache {
            Cache(kind: .lru(capacity: capacity), handle: leveldb_cache_create_lru(capacity)!)
        }

        public static func == (lhs: Cache, rhs: Cache) -> Bool {
            lhs.kind == rhs.kind
        }
    }

    public final class FilterPolicy: @unchecked Sendable, Equatable {
        public enum Kind: Equatable, Sendable {
            case bloom(bitsPerKey: Int)
            case custom(name: String)
        }

        public let kind: Kind
        fileprivate let handle: OpaquePointer

        private init(kind: Kind, handle: OpaquePointer) {
            self.kind = kind
            self.handle = handle
        }

        deinit {
            leveldb_filterpolicy_destroy(handle)
        }

        public static func bloom(bitsPerKey: Int) -> FilterPolicy {
            FilterPolicy(
                kind: .bloom(bitsPerKey: bitsPerKey),
                handle: leveldb_filterpolicy_create_bloom(Int32(bitsPerKey))!
            )
        }

        /// Creates a custom LevelDB filter policy.
        ///
        /// The policy name is a persisted compatibility contract for a database. Keep it stable,
        /// non-empty, and change it whenever the filter encoding or matching semantics change.
        /// Callback inputs are copied into Swift-owned `Data` before invocation. Returned filter
        /// bytes are copied into memory that LevelDB's C API releases after consuming them.
        public static func custom(
            name: String,
            createFilter: @escaping @Sendable ([Data]) -> Data,
            keyMayMatch: @escaping @Sendable (_ key: Data, _ filter: Data) -> Bool
        ) -> FilterPolicy {
            let state = FilterPolicyState(
                name: name,
                createFilter: createFilter,
                keyMayMatch: keyMayMatch
            )
            let retainedState = Unmanaged.passRetained(state)
            let handle = leveldb_filterpolicy_create(
                retainedState.toOpaque(),
                { statePointer in
                    Unmanaged<FilterPolicyState>.fromOpaque(statePointer!).release()
                },
                { statePointer, keyArray, keyLengthArray, keyCount, filterLengthPointer in
                    let state = Unmanaged<FilterPolicyState>
                        .fromOpaque(statePointer!)
                        .takeUnretainedValue()
                    let keys = (0..<Int(keyCount)).map { index in
                        let count = keyLengthArray![index]
                        return Data(bytes: keyArray![index]!, count: count)
                    }

                    let filter = state.createFilter(keys)
                    filterLengthPointer?.pointee = filter.count
                    let pointer = malloc(max(filter.count, 1))!.assumingMemoryBound(to: CChar.self)
                    if filter.isEmpty {
                        pointer.initialize(to: 0)
                    } else {
                        filter.withUnsafeBytes { buffer in
                            pointer.initialize(
                                from: buffer.baseAddress!.assumingMemoryBound(to: CChar.self),
                                count: filter.count
                            )
                        }
                    }
                    return pointer
                },
                { statePointer, keyPointer, keyCount, filterPointer, filterCount in
                    let state = Unmanaged<FilterPolicyState>
                        .fromOpaque(statePointer!)
                        .takeUnretainedValue()
                    let key = Data(bytes: keyPointer!, count: keyCount)
                    let filter = Data(bytes: filterPointer!, count: filterCount)
                    return state.keyMayMatch(key, filter).levelDBBool
                },
                { statePointer in
                    let state = Unmanaged<FilterPolicyState>
                        .fromOpaque(statePointer!)
                        .takeUnretainedValue()
                    return UnsafePointer(state.namePointer)
                }
            )!

            return FilterPolicy(kind: .custom(name: name), handle: handle)
        }

        public static func == (lhs: FilterPolicy, rhs: FilterPolicy) -> Bool {
            lhs.kind == rhs.kind
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

    public struct KeyRange: Equatable, Sendable {
        public var start: Data
        public var limit: Data

        public init(start: Data, limit: Data) {
            self.start = start
            self.limit = limit
        }

        public init(start: String, limit: String) {
            self.init(start: Data(start.utf8), limit: Data(limit.utf8))
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
            let key = leveldb_iter_key(handle, &keyCount)!

            return Data(bytes: key, count: keyCount)
        }

        public var value: Data? {
            guard isValid else { return nil }

            var valueCount = 0
            let value = leveldb_iter_value(handle, &valueCount)!

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

            try Database.throwIfOperationFailed(error)
        }
    }

    private let handle: OpaquePointer
    private let openOptionResources: [AnyObject]

    public convenience init(path: String, createIfMissing: Bool = true) throws {
        try self.init(
            path: path,
            options: OpenOptions(createIfMissing: createIfMissing)
        )
    }

    public init(path: String, options: OpenOptions) throws {
        let rawOptions = Self.makeOpenOptions(options)
        defer { leveldb_options_destroy(rawOptions.handle) }

        var error: UnsafeMutablePointer<CChar>?
        let database = path.withCString { pathPointer in
            leveldb_open(rawOptions.handle, pathPointer, &error)
        }

        try Self.throwIfOpenFailed(error)

        handle = try Self.unwrapOpenedDatabase(database)
        openOptionResources = rawOptions.resources
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

        try Self.throwIfOperationFailed(error)
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

        try Self.throwIfOperationFailed(error)
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

        try Self.throwIfOperationFailed(error)

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

        try Self.throwIfOperationFailed(error)
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

    public func property(_ name: String) -> String? {
        let value = name.withCString { namePointer in
            leveldb_property_value(handle, namePointer)
        }
        defer {
            if let value {
                leveldb_free(value)
            }
        }

        guard let value else {
            return nil
        }

        return String(cString: value)
    }

    public func approximateSize(of range: KeyRange) -> UInt64 {
        approximateSizes(of: [range])[0]
    }

    public func approximateSizes(of ranges: [KeyRange]) -> [UInt64] {
        guard !ranges.isEmpty else {
            return []
        }

        let preparedStarts = ranges.map { Self.copyLevelDBBytes($0.start) }
        let preparedLimits = ranges.map { Self.copyLevelDBBytes($0.limit) }
        defer {
            preparedStarts.forEach { $0.pointer.deallocate() }
            preparedLimits.forEach { $0.pointer.deallocate() }
        }

        var startPointers: [UnsafePointer<CChar>?] = preparedStarts.map { UnsafePointer($0.pointer) }
        var startLengths = preparedStarts.map(\.count)
        var limitPointers: [UnsafePointer<CChar>?] = preparedLimits.map { UnsafePointer($0.pointer) }
        var limitLengths = preparedLimits.map(\.count)
        var sizes = Array(repeating: UInt64(0), count: ranges.count)

        startPointers.withUnsafeMutableBufferPointer { startPointersBuffer in
            startLengths.withUnsafeMutableBufferPointer { startLengthsBuffer in
                limitPointers.withUnsafeMutableBufferPointer { limitPointersBuffer in
                    limitLengths.withUnsafeMutableBufferPointer { limitLengthsBuffer in
                        sizes.withUnsafeMutableBufferPointer { sizesBuffer in
                            leveldb_approximate_sizes(
                                handle,
                                Int32(ranges.count),
                                startPointersBuffer.baseAddress,
                                startLengthsBuffer.baseAddress,
                                limitPointersBuffer.baseAddress,
                                limitLengthsBuffer.baseAddress,
                                sizesBuffer.baseAddress
                            )
                        }
                    }
                }
            }
        }

        return sizes
    }

    public func compactRange(start: Data? = nil, limit: Data? = nil) {
        withOptionalLevelDBBytes(start) { startPointer, startCount in
            withOptionalLevelDBBytes(limit) { limitPointer, limitCount in
                leveldb_compact_range(handle, startPointer, startCount, limitPointer, limitCount)
            }
        }
    }

    public func compactRange(start: String?, limit: String?) {
        compactRange(
            start: start.map { Data($0.utf8) },
            limit: limit.map { Data($0.utf8) }
        )
    }

    public static func destroy(path: String, options: OpenOptions = .default) throws {
        let rawOptions = makeOpenOptions(options)
        defer { leveldb_options_destroy(rawOptions.handle) }

        var error: UnsafeMutablePointer<CChar>?
        path.withCString { pathPointer in
            leveldb_destroy_db(rawOptions.handle, pathPointer, &error)
        }

        try throwIfOperationFailed(error)
    }

    public static func repair(path: String, options: OpenOptions = .default) throws {
        let rawOptions = makeOpenOptions(options)
        defer { leveldb_options_destroy(rawOptions.handle) }

        var error: UnsafeMutablePointer<CChar>?
        path.withCString { pathPointer in
            leveldb_repair_db(rawOptions.handle, pathPointer, &error)
        }

        try throwIfOperationFailed(error)
    }

    private static func makeOpenOptions(_ options: OpenOptions) -> (handle: OpaquePointer, resources: [AnyObject]) {
        let rawOptions = leveldb_options_create()!
        var resources: [AnyObject] = []
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
        if let compression = options.compression {
            leveldb_options_set_compression(rawOptions, compression.levelDBCompression)
        }
        if let cache = options.cache {
            leveldb_options_set_cache(rawOptions, cache.handle)
            resources.append(cache)
        }
        if let filterPolicy = options.filterPolicy {
            leveldb_options_set_filter_policy(rawOptions, filterPolicy.handle)
            resources.append(filterPolicy)
        }
        if let comparator = options.comparator {
            leveldb_options_set_comparator(rawOptions, comparator.handle)
            resources.append(comparator)
        }
        if let environment = options.environment {
            leveldb_options_set_env(rawOptions, environment.handle)
            resources.append(environment)
        }
        return (rawOptions, resources)
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

    private static func throwIfOpenFailed(_ error: UnsafeMutablePointer<CChar>?) throws {
        if let error {
            throw LevelDBError.openFailed(consume(error))
        }
    }

    private static func throwIfOperationFailed(_ error: UnsafeMutablePointer<CChar>?) throws {
        if let error {
            throw LevelDBError.operationFailed(consume(error))
        }
    }

    private static func unwrapOpenedDatabase(_ database: OpaquePointer?) throws -> OpaquePointer {
        guard let database else {
            throw LevelDBError.openFailed("LevelDB did not return a database handle.")
        }
        return database
    }

    private static func copyLevelDBBytes(_ data: Data) -> (pointer: UnsafeMutablePointer<CChar>, count: Int) {
        let count = data.count
        let pointer = UnsafeMutablePointer<CChar>.allocate(capacity: max(count, 1))
        if count > 0 {
            data.withUnsafeBytes { buffer in
                pointer.initialize(
                    from: buffer.baseAddress!.assumingMemoryBound(to: CChar.self),
                    count: count
                )
            }
        } else {
            pointer.initialize(to: 0)
        }
        return (pointer, count)
    }

    private func withOptionalLevelDBBytes<Result>(
        _ data: Data?,
        _ body: (UnsafePointer<CChar>?, Int) throws -> Result
    ) rethrows -> Result {
        guard let data else {
            return try body(nil, 0)
        }

        return try data.withLevelDBBytes(body)
    }
}

#if DEBUG
extension Database {
    static func _testingThrowOpenFailed(_ message: String?) throws {
        try throwIfOpenFailed(message.flatMap { strdup($0) })
    }

    static func _testingThrowOperationFailed(_ message: String?) throws {
        try throwIfOperationFailed(message.flatMap { strdup($0) })
    }

    static func _testingUnwrapOpenResult(_ database: OpaquePointer?) throws {
        _ = try unwrapOpenedDatabase(database)
    }
}

extension Database.Environment {
    static func _testingStringAndFreeNil() -> String? {
        stringAndFree(nil)
    }
}
#endif

private extension Bool {
    var levelDBBool: UInt8 {
        self ? 1 : 0
    }
}

private extension Database.OpenOptions.Compression {
    var levelDBCompression: Int32 {
        switch self {
        case .none:
            Int32(leveldb_no_compression)
        case .snappy:
            Int32(leveldb_snappy_compression)
        }
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
