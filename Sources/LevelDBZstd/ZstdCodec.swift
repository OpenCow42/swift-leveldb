import Foundation
import LevelDBTyped
import libzstd

public enum ZstdCodecError: Error, Equatable, CustomStringConvertible {
    case compressionFailed(String)
    case decompressionFailed(String)
    case contentSizeUnavailable
    case contentSizeTooLarge(UInt64)
    case invalidAdaptivePayload(String)

    public var description: String {
        switch self {
        case .compressionFailed(let message):
            "ZSTD compression failed: \(message)"
        case .decompressionFailed(let message):
            "ZSTD decompression failed: \(message)"
        case .contentSizeUnavailable:
            "ZSTD frame does not report its decompressed size."
        case .contentSizeTooLarge(let size):
            "ZSTD decompressed size is too large for this platform: \(size)."
        case .invalidAdaptivePayload(let message):
            "Invalid adaptive ZSTD payload: \(message)"
        }
    }
}

public enum ZstdStorageStrategy: Equatable, Sendable {
    /// Always store the compressed ZSTD frame.
    case alwaysCompress
    /// Store compressed bytes only when compression saves at least this fraction
    /// of the original encoded payload size. Values are clamped to `0...1`.
    case adaptive(minimumCompressionSavingsRatio: Double)

    fileprivate func shouldStoreCompressed(originalCount: Int, compressedCount: Int) -> Bool {
        switch self {
        case .alwaysCompress:
            return true
        case .adaptive(let minimumCompressionSavingsRatio):
            guard originalCount > 0 else {
                return false
            }

            let threshold = max(0.0, min(1.0, minimumCompressionSavingsRatio))
            let compressionRatio = Double(compressedCount) / Double(originalCount)
            let savingsRatio = 1.0 - compressionRatio
            return savingsRatio >= threshold
        }
    }
}

public struct ZstdCodec<Base: LevelDBCodec>: LevelDBCodec {
    public typealias Value = Base.Value

    public var base: Base
    public var compressionLevel: Int32
    public var storageStrategy: ZstdStorageStrategy

    public init(
        wrapping base: Base,
        compressionLevel: Int32 = 3,
        storageStrategy: ZstdStorageStrategy = .adaptive(minimumCompressionSavingsRatio: 0.10)
    ) {
        self.base = base
        self.compressionLevel = compressionLevel
        self.storageStrategy = storageStrategy
    }

    public func encode(_ value: Value) throws -> Data {
        let encoded = try base.encode(value)
        let compressed = try Self.compress(encoded, compressionLevel: compressionLevel)

        switch storageStrategy {
        case .alwaysCompress:
            return compressed
        case .adaptive:
            if storageStrategy.shouldStoreCompressed(
                originalCount: encoded.count,
                compressedCount: compressed.count
            ) {
                return Self.adaptivePayload(kind: .compressed, payload: compressed)
            }

            return Self.adaptivePayload(kind: .raw, payload: encoded)
        }
    }

    public func decode(_ data: Data) throws -> Value {
        switch try Self.encodedPayload(from: data) {
        case .legacyZstdFrame(let payload):
            return try base.decode(Self.decompress(payload))
        case .adaptiveRaw(let payload):
            return try base.decode(payload)
        case .adaptiveCompressed(let payload):
            return try base.decode(Self.decompress(payload))
        }
    }

    private enum AdaptivePayloadKind: UInt8 {
        case raw = 0
        case compressed = 1
    }

    private enum EncodedPayload {
        case legacyZstdFrame(Data)
        case adaptiveRaw(Data)
        case adaptiveCompressed(Data)
    }

    private static var adaptiveMagic: [UInt8] {
        [0x73, 0x6c, 0x64, 0x62, 0x2d, 0x7a, 0x73, 0x74, 0x64]
    }

    private static var adaptiveVersion: UInt8 {
        1
    }

    private static var adaptiveHeaderCount: Int {
        adaptiveMagic.count + 2
    }

    private static func compress(_ data: Data, compressionLevel: Int32) throws -> Data {
        let bound = ZSTD_compressBound(data.count)
        var output = Data(count: bound)

        let written = output.withUnsafeMutableBytes { outputBuffer in
            data.withUnsafeBytes { inputBuffer in
                ZSTD_compress(
                    outputBuffer.baseAddress,
                    outputBuffer.count,
                    inputBuffer.baseAddress,
                    inputBuffer.count,
                    compressionLevel
                )
            }
        }

        try checkCompressionResult(written)

        output.removeSubrange(written..<output.count)
        return output
    }

    private static func adaptivePayload(kind: AdaptivePayloadKind, payload: Data) -> Data {
        var encoded = Data(adaptiveMagic)
        encoded.append(adaptiveVersion)
        encoded.append(kind.rawValue)
        encoded.append(payload)
        return encoded
    }

    private static func encodedPayload(from data: Data) throws -> EncodedPayload {
        guard data.starts(with: adaptiveMagic) else {
            return .legacyZstdFrame(data)
        }

        guard data.count >= adaptiveHeaderCount else {
            throw ZstdCodecError.invalidAdaptivePayload("Envelope header is incomplete.")
        }

        let version = data[adaptiveMagic.count]
        guard version == adaptiveVersion else {
            throw ZstdCodecError.invalidAdaptivePayload("Unsupported envelope version \(version).")
        }

        let kindByte = data[adaptiveMagic.count + 1]
        let payload = data.dropFirst(adaptiveHeaderCount)
        guard let kind = AdaptivePayloadKind(rawValue: kindByte) else {
            throw ZstdCodecError.invalidAdaptivePayload("Unknown storage tag \(kindByte).")
        }

        switch kind {
        case .raw:
            return .adaptiveRaw(Data(payload))
        case .compressed:
            return .adaptiveCompressed(Data(payload))
        }
    }

    private static func decompress(_ data: Data) throws -> Data {
        let contentSize = data.withUnsafeBytes { inputBuffer in
            ZSTD_getFrameContentSize(inputBuffer.baseAddress, inputBuffer.count)
        }

        var output = Data(count: try decompressedSize(from: contentSize))
        let read = output.withUnsafeMutableBytes { outputBuffer in
            data.withUnsafeBytes { inputBuffer in
                ZSTD_decompress(
                    outputBuffer.baseAddress,
                    outputBuffer.count,
                    inputBuffer.baseAddress,
                    inputBuffer.count
                )
            }
        }

        try checkDecompressionResult(read)

        return output
    }

    private static func checkCompressionResult(_ code: Int) throws {
        if ZSTD_isError(code) != 0 {
            throw ZstdCodecError.compressionFailed(errorName(for: code))
        }
    }

    private static func decompressedSize(from contentSize: UInt64) throws -> Int {
        if contentSize == ZSTD_CONTENTSIZE_ERROR {
            throw ZstdCodecError.decompressionFailed("Input is not a valid ZSTD frame.")
        }

        if contentSize == ZSTD_CONTENTSIZE_UNKNOWN {
            throw ZstdCodecError.contentSizeUnavailable
        }

        guard contentSize <= UInt64(Int.max) else {
            throw ZstdCodecError.contentSizeTooLarge(contentSize)
        }

        return Int(contentSize)
    }

    private static func checkDecompressionResult(_ code: Int) throws {
        if ZSTD_isError(code) != 0 {
            throw ZstdCodecError.decompressionFailed(errorName(for: code))
        }
    }

    private static func errorName(for code: Int) -> String {
        errorName(from: ZSTD_getErrorName(code), code: code)
    }

    private static func errorName(from name: UnsafePointer<CChar>?, code: Int) -> String {
        guard let name else {
            return "Unknown error code \(code)."
        }

        return String(cString: name)
    }
}

#if DEBUG
extension ZstdCodec {
    static func _testingAdaptivePayload(kind: UInt8, payload: Data) -> Data {
        var encoded = Data(adaptiveMagic)
        encoded.append(adaptiveVersion)
        encoded.append(kind)
        encoded.append(payload)
        return encoded
    }

    static func _testingAdaptivePayload(version: UInt8, kind: UInt8, payload: Data) -> Data {
        var encoded = Data(adaptiveMagic)
        encoded.append(version)
        encoded.append(kind)
        encoded.append(payload)
        return encoded
    }

    static func _testingIncompleteAdaptiveEnvelope() -> Data {
        Data(adaptiveMagic)
    }

    static func _testingShouldStoreCompressed(
        strategy: ZstdStorageStrategy,
        originalCount: Int,
        compressedCount: Int
    ) -> Bool {
        strategy.shouldStoreCompressed(
            originalCount: originalCount,
            compressedCount: compressedCount
        )
    }

    static func _testingCheckCompressionResult(_ code: Int) throws {
        try checkCompressionResult(code)
    }

    static func _testingDecompressedSize(from contentSize: UInt64) throws -> Int {
        try decompressedSize(from: contentSize)
    }

    static func _testingCheckDecompressionResult(_ code: Int) throws {
        try checkDecompressionResult(code)
    }

    static func _testingUnknownErrorName(code: Int) -> String {
        errorName(from: nil, code: code)
    }
}
#endif
