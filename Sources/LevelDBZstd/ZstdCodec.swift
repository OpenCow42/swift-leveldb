import Foundation
import LevelDBTyped
import libzstd

public enum ZstdCodecError: Error, Equatable, CustomStringConvertible {
    case compressionFailed(String)
    case decompressionFailed(String)
    case contentSizeUnavailable
    case contentSizeTooLarge(UInt64)

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
        }
    }
}

public struct ZstdCodec<Base: LevelDBCodec>: LevelDBCodec {
    public typealias Value = Base.Value

    public var base: Base
    public var compressionLevel: Int32

    public init(wrapping base: Base, compressionLevel: Int32 = 3) {
        self.base = base
        self.compressionLevel = compressionLevel
    }

    public func encode(_ value: Value) throws -> Data {
        let encoded = try base.encode(value)
        return try Self.compress(encoded, compressionLevel: compressionLevel)
    }

    public func decode(_ data: Data) throws -> Value {
        let decompressed = try Self.decompress(data)
        return try base.decode(decompressed)
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
