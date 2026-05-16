import Foundation
import LevelDBTyped
import Testing
@testable import LevelDBZstd

@Test func zstdErrorDescriptionsAreStable() {
    #expect(ZstdCodecError.compressionFailed("bad").description == "ZSTD compression failed: bad")
    #expect(ZstdCodecError.decompressionFailed("bad").description == "ZSTD decompression failed: bad")
    #expect(ZstdCodecError.contentSizeUnavailable.description == "ZSTD frame does not report its decompressed size.")
    #expect(ZstdCodecError.contentSizeTooLarge(9).description == "ZSTD decompressed size is too large for this platform: 9.")
}

@Test func zstdDecodeRejectsInvalidFrame() throws {
    let codec = ZstdCodec(wrapping: DataCodec())

    do {
        _ = try codec.decode(Data("not-zstd".utf8))
        Issue.record("Expected invalid ZSTD frame to fail")
    } catch ZstdCodecError.decompressionFailed(let message) {
        #expect(message == "Input is not a valid ZSTD frame.")
    }
}

@Test func zstdInternalErrorHelpersCoverFailureBranches() throws {
    do {
        try ZstdCodec<DataCodec>._testingCheckCompressionResult(-1)
        Issue.record("Expected compression result to fail")
    } catch ZstdCodecError.compressionFailed(let message) {
        #expect(!message.isEmpty)
    }
    try ZstdCodec<DataCodec>._testingCheckCompressionResult(0)

    do {
        _ = try ZstdCodec<DataCodec>._testingDecompressedSize(from: UInt64.max)
        Issue.record("Expected unknown content size to fail")
    } catch ZstdCodecError.contentSizeUnavailable {
        // Expected.
    }

    do {
        _ = try ZstdCodec<DataCodec>._testingDecompressedSize(from: UInt64(Int.max) + 1)
        Issue.record("Expected oversized content size to fail")
    } catch ZstdCodecError.contentSizeTooLarge(let size) {
        #expect(size == UInt64(Int.max) + 1)
    }
    #expect(try ZstdCodec<DataCodec>._testingDecompressedSize(from: 3) == 3)

    do {
        try ZstdCodec<DataCodec>._testingCheckDecompressionResult(-1)
        Issue.record("Expected decompression result to fail")
    } catch ZstdCodecError.decompressionFailed(let message) {
        #expect(!message.isEmpty)
    }
    try ZstdCodec<DataCodec>._testingCheckDecompressionResult(0)

    #expect(ZstdCodec<DataCodec>._testingUnknownErrorName(code: 123) == "Unknown error code 123.")
}

@Test func zstdRandomRoundTripsRawData() throws {
    let codec = ZstdCodec(wrapping: DataCodec(), compressionLevel: 5)
    var random = ZstdCoverageRandom(seed: 99)

    for _ in 0..<50 {
        let payload = randomPayload(random: &random)
        let encoded = try codec.encode(payload)
        #expect(try codec.decode(encoded) == payload)
    }
}

@Test func zstdRandomRoundTripsStrings() throws {
    let codec = ZstdCodec(wrapping: StringCodec(), compressionLevel: 1)
    var random = ZstdCoverageRandom(seed: 101)

    for _ in 0..<50 {
        let value = String(decoding: randomPayload(random: &random), as: UTF8.self)
        let encoded = try codec.encode(value)
        #expect(try codec.decode(encoded) == value)
    }
}

private struct ZstdCoverageRandom {
    private var state: UInt64

    init(seed: UInt64) {
        state = seed
    }

    mutating func next() -> UInt64 {
        state = state &* 6_364_136_223_846_793_005 &+ 1
        return state
    }

    mutating func uniform(_ upperBound: Int) -> Int {
        Int(next() % UInt64(upperBound))
    }
}

private func randomPayload(random: inout ZstdCoverageRandom) -> Data {
    let count = 1 + random.uniform(512)
    let compressible = random.uniform(2) == 0
    return Data((0..<count).map { index in
        compressible ? UInt8(index % 8) : UInt8(random.uniform(256))
    })
}
