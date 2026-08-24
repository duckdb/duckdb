//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/file_compression_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

//! FileCompressionType represents a file compression scheme by its name (e.g. "gzip"), so extensions can register
//! their own compression filesystems in addition to the built-in ones
class FileCompressionType {
public:
	//! Creates an UNCOMPRESSED compression type
	FileCompressionType() : compression("uncompressed") {
	}
	//! Creates a compression type from a string, normalizing known aliases (e.g. "infer" -> auto detection)
	DUCKDB_API FileCompressionType(string compression); // NOLINT: allow implicit conversion from string

	//! Detect the compression type based on the file name
	DUCKDB_API static const FileCompressionType AUTO_DETECT;
	//! No compression
	DUCKDB_API static const FileCompressionType UNCOMPRESSED;
	//! Built-in compression types
	DUCKDB_API static const FileCompressionType GZIP;
	DUCKDB_API static const FileCompressionType ZSTD;

public:
	bool operator==(const FileCompressionType &other) const {
		return compression == other.compression;
	}
	bool operator!=(const FileCompressionType &other) const {
		return compression != other.compression;
	}
	//! Whether this refers to an actual compression scheme (i.e. not uncompressed and not auto-detection)
	bool IsCompressed() const {
		return !IsUncompressed() && !IsAutoDetect();
	}
	bool IsUncompressed() const {
		return compression == "uncompressed";
	}
	bool IsAutoDetect() const {
		return compression == "auto_detect";
	}
	//! The canonical (lower-case) name of the compression scheme, e.g. "gzip"
	const string &ToString() const {
		return compression;
	}

private:
	string compression;
};

DUCKDB_API FileCompressionType FileCompressionTypeFromString(const string &input);

DUCKDB_API string CompressionExtensionFromType(const FileCompressionType &type);

DUCKDB_API bool IsFileCompressed(string path, const FileCompressionType &type);

} // namespace duckdb
