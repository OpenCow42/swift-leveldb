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

public final class Database {
    private let handle: OpaquePointer

    public init(path: String, createIfMissing: Bool = true) throws {
        let options = leveldb_options_create()
        defer { leveldb_options_destroy(options) }

        leveldb_options_set_create_if_missing(options, createIfMissing ? 1 : 0)

        var error: UnsafeMutablePointer<CChar>?
        let database = path.withCString { pathPointer in
            leveldb_open(options, pathPointer, &error)
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

    public func put(_ value: Data, forKey key: Data, sync: Bool = false) throws {
        let options = leveldb_writeoptions_create()
        defer { leveldb_writeoptions_destroy(options) }

        leveldb_writeoptions_set_sync(options, sync ? 1 : 0)

        var error: UnsafeMutablePointer<CChar>?
        key.withLevelDBBytes { keyPointer, keyCount in
            value.withLevelDBBytes { valuePointer, valueCount in
                leveldb_put(
                    handle,
                    options,
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
        let options = leveldb_readoptions_create()
        defer { leveldb_readoptions_destroy(options) }

        var error: UnsafeMutablePointer<CChar>?
        var valueCount = 0
        let value = key.withLevelDBBytes { keyPointer, keyCount in
            leveldb_get(handle, options, keyPointer, keyCount, &valueCount, &error)
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
        guard let data = try get(Data(key.utf8)) else {
            return nil
        }

        return String(decoding: data, as: UTF8.self)
    }

    public func deleteValue(forKey key: Data, sync: Bool = false) throws {
        let options = leveldb_writeoptions_create()
        defer { leveldb_writeoptions_destroy(options) }

        leveldb_writeoptions_set_sync(options, sync ? 1 : 0)

        var error: UnsafeMutablePointer<CChar>?
        key.withLevelDBBytes { keyPointer, keyCount in
            leveldb_delete(handle, options, keyPointer, keyCount, &error)
        }

        if let error {
            throw LevelDBError.operationFailed(Self.consume(error))
        }
    }

    public func deleteValue(forKey key: String, sync: Bool = false) throws {
        try deleteValue(forKey: Data(key.utf8), sync: sync)
    }

    private static func consume(_ error: UnsafeMutablePointer<CChar>) -> String {
        let message = String(cString: error)
        leveldb_free(error)
        return message
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
