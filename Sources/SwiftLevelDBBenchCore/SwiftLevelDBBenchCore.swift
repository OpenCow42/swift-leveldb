import Foundation
import LevelDBTyped
import LevelDBZstd
import swift_leveldb

public struct SwiftLevelDBBenchmarkOptions: Sendable {
    public var benchmarks: [String]
    public var num: Int
    public var reads: Int
    public var valueSize: Int
    public var compressionRatio: Double
    public var databasePath: String?
    public var useExistingDatabase: Bool
    public var cacheSize: Int?
    public var bloomBits: Int?
    public var json: Bool

    public init(
        benchmarks: [String] = ["fillseq", "fillrandom", "readrandom"],
        num: Int = 1_000_000,
        reads: Int? = nil,
        valueSize: Int = 100,
        compressionRatio: Double = 0.5,
        databasePath: String? = nil,
        useExistingDatabase: Bool = false,
        cacheSize: Int? = nil,
        bloomBits: Int? = nil,
        json: Bool = false
    ) {
        self.benchmarks = benchmarks
        self.num = num
        self.reads = reads ?? num
        self.valueSize = valueSize
        self.compressionRatio = compressionRatio
        self.databasePath = databasePath
        self.useExistingDatabase = useExistingDatabase
        self.cacheSize = cacheSize
        self.bloomBits = bloomBits
        self.json = json
    }
}

public struct SwiftLevelDBBenchmarkResult: Codable, Equatable, Sendable {
    public var name: String
    public var operations: Int
    public var microsecondsPerOperation: Double
    public var megabytesPerSecond: Double?

    public init(
        name: String,
        operations: Int,
        microsecondsPerOperation: Double,
        megabytesPerSecond: Double? = nil
    ) {
        self.name = name
        self.operations = operations
        self.microsecondsPerOperation = microsecondsPerOperation
        self.megabytesPerSecond = megabytesPerSecond
    }
}

public enum SwiftLevelDBBenchmarkError: Error, CustomStringConvertible {
    case invalidArgument(String)
    case unknownBenchmark(String)

    public var description: String {
        switch self {
        case .invalidArgument(let argument):
            "Invalid benchmark argument: \(argument)"
        case .unknownBenchmark(let name):
            "Unknown benchmark: \(name)"
        }
    }
}

public enum SwiftLevelDBBenchmark {
    public static func parse(arguments: [String]) throws -> SwiftLevelDBBenchmarkOptions {
        var options = SwiftLevelDBBenchmarkOptions()

        for argument in arguments {
            if let value = argument.value(for: "--benchmarks=") {
                options.benchmarks = value.split(separator: ",").map(String.init)
            } else if let value = argument.value(for: "--num=") {
                options.num = try intValue(value, argument: argument)
                if options.reads == 1_000_000 {
                    options.reads = options.num
                }
            } else if let value = argument.value(for: "--reads=") {
                options.reads = try intValue(value, argument: argument)
            } else if let value = argument.value(for: "--value-size=") {
                options.valueSize = try intValue(value, argument: argument)
            } else if let value = argument.value(for: "--compression-ratio=") {
                guard let ratio = Double(value) else {
                    throw SwiftLevelDBBenchmarkError.invalidArgument(argument)
                }
                options.compressionRatio = ratio
            } else if let value = argument.value(for: "--db=") {
                options.databasePath = value
            } else if argument == "--use-existing-db" {
                options.useExistingDatabase = true
            } else if let value = argument.value(for: "--use-existing-db=") {
                options.useExistingDatabase = value != "0" && value != "false"
            } else if let value = argument.value(for: "--cache-size=") {
                options.cacheSize = try intValue(value, argument: argument)
            } else if let value = argument.value(for: "--bloom-bits=") {
                options.bloomBits = try intValue(value, argument: argument)
            } else if argument == "--json" {
                options.json = true
            } else {
                throw SwiftLevelDBBenchmarkError.invalidArgument(argument)
            }
        }

        return options
    }

    public static func run(arguments: [String]) async throws -> [SwiftLevelDBBenchmarkResult] {
        try await run(options: parse(arguments: arguments))
    }

    public static func run(options: SwiftLevelDBBenchmarkOptions) async throws -> [SwiftLevelDBBenchmarkResult] {
        var results: [SwiftLevelDBBenchmarkResult] = []
        for benchmark in options.benchmarks {
            results.append(try await runOne(benchmark, options: options))
        }
        return results
    }

    public static func render(_ results: [SwiftLevelDBBenchmarkResult], json: Bool) throws -> String {
        if json {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            return String(decoding: try encoder.encode(results), as: UTF8.self)
        }

        var lines = ["benchmark              ops      micros/op       MB/s"]
        for result in results {
            let speed = result.megabytesPerSecond.map { String(format: "%10.2f", $0) } ?? "         -"
            let name = result.name.padding(toLength: 18, withPad: " ", startingAt: 0)
            lines.append("\(name) \(String(format: "%8d", result.operations)) \(String(format: "%14.3f", result.microsecondsPerOperation)) \(speed)")
        }
        return lines.joined(separator: "\n")
    }

    private static func runOne(
        _ name: String,
        options: SwiftLevelDBBenchmarkOptions
    ) async throws -> SwiftLevelDBBenchmarkResult {
        switch name {
        case "fillseq":
            return try measureDatabase(name: name, options: options, populate: false) { database in
                let generator = ValueGenerator(valueSize: options.valueSize, compressionRatio: options.compressionRatio)
                for index in 0..<options.num {
                    try database.put(generator.value(for: index), forKey: key(index))
                }
                return (options.num, options.num * (16 + options.valueSize))
            }
        case "fillrandom":
            return try measureDatabase(name: name, options: options, populate: false) { database in
                var random = SeededRandom(seed: 301)
                let generator = ValueGenerator(valueSize: options.valueSize, compressionRatio: options.compressionRatio)
                for index in 0..<options.num {
                    try database.put(generator.value(for: index), forKey: key(random.uniform(options.num)))
                }
                return (options.num, options.num * (16 + options.valueSize))
            }
        case "overwrite":
            return try measureDatabase(name: name, options: options, populate: true) { database in
                var random = SeededRandom(seed: 302)
                let generator = ValueGenerator(valueSize: options.valueSize, compressionRatio: options.compressionRatio)
                for index in 0..<options.num {
                    try database.put(generator.value(for: index), forKey: key(random.uniform(options.num)))
                }
                return (options.num, options.num * (16 + options.valueSize))
            }
        case "readseq":
            return try measureDatabase(name: name, options: options, populate: true) { database in
                let iterator = database.makeIterator()
                var count = 0
                iterator.seekToFirst()
                while iterator.isValid, count < options.reads {
                    _ = iterator.key
                    _ = iterator.value
                    iterator.next()
                    count += 1
                }
                try iterator.checkError()
                return (count, count * options.valueSize)
            }
        case "readreverse":
            return try measureDatabase(name: name, options: options, populate: true) { database in
                let iterator = database.makeIterator()
                var count = 0
                iterator.seekToLast()
                while iterator.isValid, count < options.reads {
                    _ = iterator.key
                    _ = iterator.value
                    iterator.previous()
                    count += 1
                }
                try iterator.checkError()
                return (count, count * options.valueSize)
            }
        case "readrandom":
            return try measureDatabase(name: name, options: options, populate: true) { database in
                var random = SeededRandom(seed: 303)
                for _ in 0..<options.reads {
                    _ = try database.get(key(random.uniform(options.num)))
                }
                return (options.reads, options.reads * options.valueSize)
            }
        case "seekrandom":
            return try measureDatabase(name: name, options: options, populate: true) { database in
                var random = SeededRandom(seed: 304)
                let iterator = database.makeIterator()
                for _ in 0..<options.reads {
                    iterator.seek(key(random.uniform(options.num)))
                    _ = iterator.key
                }
                try iterator.checkError()
                return (options.reads, 0)
            }
        case "deleterandom":
            return try measureDatabase(name: name, options: options, populate: true) { database in
                var random = SeededRandom(seed: 305)
                for _ in 0..<options.num {
                    try database.deleteValue(forKey: key(random.uniform(options.num)))
                }
                return (options.num, 0)
            }
        case "typedjson", "typed-json":
            return try await measureTypedJSON(name: name, options: options)
        case "zstd", "zstd-json":
            return try await measureZstdJSON(name: name, options: options)
        default:
            throw SwiftLevelDBBenchmarkError.unknownBenchmark(name)
        }
    }

    private static func measureDatabase(
        name: String,
        options: SwiftLevelDBBenchmarkOptions,
        populate: Bool,
        body: (Database) throws -> (operations: Int, bytes: Int)
    ) throws -> SwiftLevelDBBenchmarkResult {
        let directory = benchmarkDirectory(options: options, name: name)
        if !options.useExistingDatabase {
            try? FileManager.default.removeItem(at: directory)
        }
        defer {
            if options.databasePath == nil {
                try? FileManager.default.removeItem(at: directory)
            }
        }

        let database = try Database(path: directory.path, options: openOptions(options))
        if populate {
            try populateDatabase(database, options: options)
        }

        let started = DispatchTime.now().uptimeNanoseconds
        let measured = try body(database)
        let elapsed = DispatchTime.now().uptimeNanoseconds - started
        return result(name: name, operations: measured.operations, bytes: measured.bytes, elapsedNanoseconds: elapsed)
    }

    private static func measureTypedJSON(
        name: String,
        options: SwiftLevelDBBenchmarkOptions
    ) async throws -> SwiftLevelDBBenchmarkResult {
        let directory = benchmarkDirectory(options: options, name: name)
        if !options.useExistingDatabase {
            try? FileManager.default.removeItem(at: directory)
        }
        defer {
            if options.databasePath == nil {
                try? FileManager.default.removeItem(at: directory)
            }
        }

        let store = try LevelDBStores.json(path: directory.path, valueType: BenchRecord.self)
        let payload = String(repeating: "x", count: options.valueSize)
        let started = DispatchTime.now().uptimeNanoseconds
        for index in 0..<options.num {
            try await store.put(BenchRecord(id: index, payload: payload), forKey: keyString(index))
        }
        for index in 0..<options.reads {
            _ = try await store.value(forKey: keyString(index % options.num))
        }
        let elapsed = DispatchTime.now().uptimeNanoseconds - started
        return result(
            name: name,
            operations: options.num + options.reads,
            bytes: (options.num + options.reads) * options.valueSize,
            elapsedNanoseconds: elapsed
        )
    }

    private static func measureZstdJSON(
        name: String,
        options: SwiftLevelDBBenchmarkOptions
    ) async throws -> SwiftLevelDBBenchmarkResult {
        let directory = benchmarkDirectory(options: options, name: name)
        if !options.useExistingDatabase {
            try? FileManager.default.removeItem(at: directory)
        }
        defer {
            if options.databasePath == nil {
                try? FileManager.default.removeItem(at: directory)
            }
        }

        let store = try LevelDBStore(
            path: directory.path,
            keyCodec: StringCodec(),
            valueCodec: ZstdCodec(wrapping: JSONCodec<BenchRecord>())
        )
        let payload = String(repeating: "z", count: options.valueSize)
        let started = DispatchTime.now().uptimeNanoseconds
        for index in 0..<options.num {
            try await store.put(BenchRecord(id: index, payload: payload), forKey: keyString(index))
        }
        for index in 0..<options.reads {
            _ = try await store.value(forKey: keyString(index % options.num))
        }
        let elapsed = DispatchTime.now().uptimeNanoseconds - started
        return result(
            name: name,
            operations: options.num + options.reads,
            bytes: (options.num + options.reads) * options.valueSize,
            elapsedNanoseconds: elapsed
        )
    }

    private static func populateDatabase(_ database: Database, options: SwiftLevelDBBenchmarkOptions) throws {
        let generator = ValueGenerator(valueSize: options.valueSize, compressionRatio: options.compressionRatio)
        for index in 0..<options.num {
            try database.put(generator.value(for: index), forKey: key(index))
        }
    }

    private static func openOptions(_ options: SwiftLevelDBBenchmarkOptions) -> Database.OpenOptions {
        Database.OpenOptions(
            cache: options.cacheSize.map(Database.Cache.lru(capacity:)),
            filterPolicy: options.bloomBits.map(Database.FilterPolicy.bloom(bitsPerKey:))
        )
    }

    private static func benchmarkDirectory(options: SwiftLevelDBBenchmarkOptions, name: String) -> URL {
        if let databasePath = options.databasePath {
            return URL(fileURLWithPath: databasePath).appendingPathComponent(name)
        }
        return FileManager.default.temporaryDirectory
            .appendingPathComponent("swift-leveldb-bench-\(name)-\(UUID().uuidString)")
    }

    private static func key(_ index: Int) -> Data {
        Data(keyString(index).utf8)
    }

    private static func keyString(_ index: Int) -> String {
        String(format: "%016d", index)
    }

    private static func result(
        name: String,
        operations: Int,
        bytes: Int,
        elapsedNanoseconds: UInt64
    ) -> SwiftLevelDBBenchmarkResult {
        let seconds = max(Double(elapsedNanoseconds) / 1_000_000_000, .leastNonzeroMagnitude)
        let micros = seconds * 1_000_000 / Double(max(operations, 1))
        let megabytes = bytes > 0 ? Double(bytes) / 1_048_576 / seconds : nil
        return SwiftLevelDBBenchmarkResult(
            name: name,
            operations: operations,
            microsecondsPerOperation: micros,
            megabytesPerSecond: megabytes
        )
    }

    private static func intValue(_ value: String, argument: String) throws -> Int {
        guard let intValue = Int(value), intValue >= 0 else {
            throw SwiftLevelDBBenchmarkError.invalidArgument(argument)
        }
        return intValue
    }
}

private struct BenchRecord: Codable, Equatable, Sendable {
    var id: Int
    var payload: String
}

private struct ValueGenerator {
    let valueSize: Int
    let compressionRatio: Double

    func value(for index: Int) -> Data {
        let rawLength = max(1, Int(Double(valueSize) * compressionRatio))
        let seed = "value-\(index)-"
        var raw = ""
        while raw.count < rawLength {
            raw += seed
        }
        let bytes = Array(raw.utf8.prefix(rawLength))
        var output = [UInt8]()
        while output.count < valueSize {
            output.append(contentsOf: bytes)
        }
        return Data(output.prefix(valueSize))
    }
}

private struct SeededRandom {
    private var state: UInt64

    init(seed: UInt64) {
        state = seed
    }

    mutating func next() -> UInt64 {
        state = state &* 6_364_136_223_846_793_005 &+ 1_442_695_040_888_963_407
        return state
    }

    mutating func uniform(_ upperBound: Int) -> Int {
        guard upperBound > 0 else { return 0 }
        return Int(next() % UInt64(upperBound))
    }
}

private extension String {
    func value(for prefix: String) -> String? {
        guard hasPrefix(prefix) else { return nil }
        return String(dropFirst(prefix.count))
    }
}
