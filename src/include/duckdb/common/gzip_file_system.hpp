//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/gzip_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/compressed_file_system.hpp"

namespace duckdb {

class GZipFileSystem : public CompressedFileSystem {
	// 32 KB
	static constexpr const idx_t BUFFER_SIZE = 1u << 15;

public:
	unique_ptr<FileHandle> OpenCompressedFile(unique_ptr<FileHandle> handle, bool write) override;

	std::string GetName() const override {
		return "GZipFileSystem";
	}

	//! Verifies that a buffer contains a valid GZIP header
	static void VerifyGZIPHeader(uint8_t gzip_hdr[], idx_t read_count);
	//! Consumes a byte stream as a gzip string, returning the decompressed string
	static string UncompressGZIPString(const string &in);

	unique_ptr<StreamWrapper> CreateStream() override;
	idx_t InBufferSize() override;
	idx_t OutBufferSize() override;
};

static constexpr const uint8_t GZIP_COMPRESSION_DEFLATE = 0x08;

static constexpr const uint8_t GZIP_FLAG_ASCII = 0x1;
static constexpr const uint8_t GZIP_FLAG_MULTIPART = 0x2;
static constexpr const uint8_t GZIP_FLAG_EXTRA = 0x4;
static constexpr const uint8_t GZIP_FLAG_NAME = 0x8;
static constexpr const uint8_t GZIP_FLAG_COMMENT = 0x10;
static constexpr const uint8_t GZIP_FLAG_ENCRYPT = 0x20;

static constexpr const uint8_t GZIP_HEADER_MINSIZE = 10;
// MAXSIZE should be the same as input buffer size
static constexpr const idx_t GZIP_HEADER_MAXSIZE = 1u << 15;
static constexpr const uint8_t GZIP_FOOTER_SIZE = 8;

static constexpr const unsigned char GZIP_FLAG_UNSUPPORTED =
    GZIP_FLAG_ASCII | GZIP_FLAG_MULTIPART | GZIP_FLAG_COMMENT | GZIP_FLAG_ENCRYPT;

} // namespace duckdb
