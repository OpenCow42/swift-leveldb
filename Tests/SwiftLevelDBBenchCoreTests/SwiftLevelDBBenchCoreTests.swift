import SwiftLevelDBBenchCore
import Testing

@Test func parsesBenchmarkArguments() throws {
    let options = try SwiftLevelDBBenchmark.parse(arguments: [
        "--benchmarks=fillseq,readrandom",
        "--num=10",
        "--reads=5",
        "--value-size=32",
        "--compression-ratio=0.75",
        "--cache-size=1024",
        "--bloom-bits=10",
        "--use-existing-db=1",
        "--json",
    ])

    #expect(options.benchmarks == ["fillseq", "readrandom"])
    #expect(options.num == 10)
    #expect(options.reads == 5)
    #expect(options.valueSize == 32)
    #expect(options.compressionRatio == 0.75)
    #expect(options.cacheSize == 1024)
    #expect(options.bloomBits == 10)
    #expect(options.useExistingDatabase)
    #expect(options.json)
}

@Test func swiftBenchmarkSmokeProducesPositiveTimings() async throws {
    let results = try await SwiftLevelDBBenchmark.run(options: SwiftLevelDBBenchmarkOptions(
        benchmarks: ["fillseq", "readrandom", "seekrandom", "typedjson", "zstd"],
        num: 8,
        reads: 4,
        valueSize: 16
    ))

    #expect(results.count == 5)
    #expect(results.allSatisfy { $0.operations > 0 })
    #expect(results.allSatisfy { $0.microsecondsPerOperation > 0 })
    #expect(try SwiftLevelDBBenchmark.render(results, json: false).contains("benchmark"))
    #expect(try SwiftLevelDBBenchmark.render(results, json: true).contains("microsecondsPerOperation"))
}
