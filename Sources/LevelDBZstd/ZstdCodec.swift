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

        if ZSTD_isError(written) != 0 {
            throw ZstdCodecError.compressionFailed(errorName(for: written))
        }

        output.removeSubrange(written..<output.count)
        return output
    }

    private static func decompress(_ data: Data) throws -> Data {
        let contentSize = data.withUnsafeBytes { inputBuffer in
            ZSTD_getFrameContentSize(inputBuffer.baseAddress, inputBuffer.count)
        }

        if contentSize == ZSTD_CONTENTSIZE_ERROR {
            throw ZstdCodecError.decompressionFailed("Input is not a valid ZSTD frame.")
        }

        if contentSize == ZSTD_CONTENTSIZE_UNKNOWN {
            throw ZstdCodecError.contentSizeUnavailable
        }

        guard contentSize <= UInt64(Int.max) else {
            throw ZstdCodecError.contentSizeTooLarge(contentSize)
        }

        var output = Data(count: Int(contentSize))
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

        if ZSTD_isError(read) != 0 {
            throw ZstdCodecError.decompressionFailed(errorName(for: read))
        }

        return output
    }

    private static func errorName(for code: Int) -> String {
        guard let name = ZSTD_getErrorName(code) else {
            return "Unknown error code \(code)."
        }

        return String(cString: name)
    }
}
