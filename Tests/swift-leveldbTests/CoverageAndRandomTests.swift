import Foundation
import Testing
@testable import swift_leveldb

@Test func errorDescriptionsExposeMessages() {
    #expect(LevelDBError.openFailed("open").description == "open")
    #expect(LevelDBError.operationFailed("operation").description == "operation")
}

@Test func errorHelpersCoverLevelDBFailureConversions() throws {
    do {
        try Database._testingThrowOpenFailed("open helper")
        Issue.record("Expected open helper to throw")
    } catch LevelDBError.openFailed(let message) {
        #expect(message == "open helper")
    }
    try Database._testingThrowOpenFailed(nil)

    do {
        try Database._testingThrowOperationFailed("operation helper")
        Issue.record("Expected operation helper to throw")
    } catch LevelDBError.operationFailed(let message) {
        #expect(message == "operation helper")
    }
    try Database._testingThrowOperationFailed(nil)

    do {
        try Database._testingUnwrapOpenResult(nil)
        Issue.record("Expected nil open result to throw")
    } catch LevelDBError.openFailed(let message) {
        #expect(message == "LevelDB did not return a database handle.")
    }
    try Database._testingUnwrapOpenResult(OpaquePointer(bitPattern: 1))
}

@Test func optionValueTypesAndResourcesCompareByConfiguration() {
    #expect(Database.OpenOptions.default == Database.OpenOptions())
    #expect(Database.ReadOptions.default == Database.ReadOptions())
    #expect(Database.WriteOptions.default == Database.WriteOptions())
    #expect(Database.KeyRange(start: "a", limit: "z") == Database.KeyRange(start: Data("a".utf8), limit: Data("z".utf8)))
    #expect(Database.Cache.lru(capacity: 1024) == Database.Cache.lru(capacity: 1024))
    #expect(Database.Cache.lru(capacity: 1024) != Database.Cache.lru(capacity: 2048))
    #expect(Database.FilterPolicy.bloom(bitsPerKey: 10) == Database.FilterPolicy.bloom(bitsPerKey: 10))
    #expect(Database.FilterPolicy.bloom(bitsPerKey: 10) != Database.FilterPolicy.bloom(bitsPerKey: 12))
    #expect(Database.Comparator.custom(name: "a") { _, _ in .orderedSame } == Database.Comparator.custom(name: "a") { _, _ in .orderedSame })
    #expect(Database.Comparator.custom(name: "a") { _, _ in .orderedSame } != Database.Comparator.custom(name: "b") { _, _ in .orderedSame })

    let firstEnvironment = Database.Environment.default
    let secondEnvironment = Database.Environment.default
    #expect(firstEnvironment == firstEnvironment)
    #expect(firstEnvironment != secondEnvironment)
    #expect(Database.Environment._testingStringAndFreeNil() == nil)
}

@Test func openOptionsNumericSettingsAndParanoidChecksOpenDatabase() throws {
    let directory = temporaryCoverageDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }

    let database = try Database(
        path: directory.path,
        options: Database.OpenOptions(
            paranoidChecks: true,
            writeBufferSize: 64 * 1024,
            maxOpenFiles: 64,
            blockSize: 4096,
            blockRestartInterval: 8,
            maxFileSize: 256 * 1024
        )
    )
    try database.put(Data(), forKey: Data(), writeOptions: Database.WriteOptions(sync: true))
    #expect(try database.get(Data(), readOptions: Database.ReadOptions(verifyChecksums: true, fillCache: false)) == Data())
}

@Test func writeBatchClearRemovesQueuedOperations() throws {
    let directory = temporaryCoverageDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)
    let batch = WriteBatch()

    batch.put("stale", forKey: "key")
    batch.clear()
    batch.put("fresh", forKey: "key")
    try database.write(batch, writeOptions: Database.WriteOptions(sync: true))

    #expect(batch.operations() == [.put(key: Data("key".utf8), value: Data("fresh".utf8))])
    #expect(try database.string(forKey: "key") == "fresh")
}

@Test func maintenanceHelpersHandleEmptyRangesAndStringCompaction() throws {
    let directory = temporaryCoverageDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)

    try database.put("1", forKey: "a")
    try database.put("2", forKey: "b")

    #expect(database.approximateSizes(of: []).isEmpty)
    database.compactRange(start: Optional<String>.none, limit: Optional<String>.none)
    database.compactRange(start: "a", limit: "c")
    #expect(try database.string(forKey: "a") == "1")
    #expect(try database.string(forKey: "b") == "2")
}

@Test func customFilterPolicyCanReturnEmptyFilterBytes() throws {
    let directory = temporaryCoverageDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let filterPolicy = Database.FilterPolicy.custom(
        name: "swift-leveldb.tests.empty-filter",
        createFilter: { _ in Data() },
        keyMayMatch: { _, filter in
            filter.isEmpty
        }
    )
    let database = try Database(
        path: directory.path,
        options: Database.OpenOptions(filterPolicy: filterPolicy)
    )

    try database.put("value", forKey: "key")
    database.compactRange()
    #expect(try database.string(forKey: "missing") == nil)
}

@Test func randomLowLevelOperationsMatchInMemoryOracle() throws {
    let directory = temporaryCoverageDatabaseDirectory()
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try Database(path: directory.path)
    var random = CoverageRandom(seed: 42)
    var oracle: [Data: Data] = [:]

    for _ in 0..<200 {
        let key = randomData(random: &random, maxCount: 12)
        switch random.uniform(4) {
        case 0:
            try database.deleteValue(forKey: key)
            oracle[key] = nil
        case 1:
            let batch = WriteBatch()
            let firstKey = randomData(random: &random, maxCount: 12)
            let firstValue = randomData(random: &random, maxCount: 24)
            let secondKey = randomData(random: &random, maxCount: 12)
            batch.put(firstValue, forKey: firstKey)
            batch.deleteValue(forKey: secondKey)
            try database.write(batch)
            oracle[firstKey] = firstValue
            oracle[secondKey] = nil
        default:
            let value = randomData(random: &random, maxCount: 24)
            try database.put(value, forKey: key)
            oracle[key] = value
        }
    }

    for (key, value) in oracle {
        #expect(try database.get(key) == value)
    }

    let iterator = database.makeIterator()
    iterator.seekToFirst()
    var scanned: [Data] = []
    while iterator.isValid {
        scanned.append(iterator.key!)
        iterator.next()
    }
    try iterator.checkError()

    #expect(scanned == oracle.keys.sorted { $0.lexicographicallyPrecedes($1) })
}

private func temporaryCoverageDatabaseDirectory() -> URL {
    FileManager.default.temporaryDirectory
        .appendingPathComponent("swift-leveldb-coverage-\(UUID().uuidString)")
}

private struct CoverageRandom {
    private var state: UInt64

    init(seed: UInt64) {
        state = seed
    }

    mutating func next() -> UInt64 {
        state = state &* 2_862_933_555_777_941_757 &+ 3_037_000_493
        return state
    }

    mutating func uniform(_ upperBound: Int) -> Int {
        Int(next() % UInt64(upperBound))
    }
}

private func randomData(random: inout CoverageRandom, maxCount: Int) -> Data {
    let count = random.uniform(maxCount + 1)
    return Data((0..<count).map { _ in UInt8(random.uniform(256)) })
}
