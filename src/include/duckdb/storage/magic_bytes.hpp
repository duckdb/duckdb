//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/magic_bytes.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class FileSystem;

enum class DataFileType : uint8_t {
	FILE_DOES_NOT_EXIST, // file does not exist
	DUCKDB_FILE,         // duckdb database file
	SQLITE_FILE,         // sqlite database file
	PARQUET_FILE         // parquet file
};

class MagicBytes {
public:
	static DataFileType CheckMagicBytes(FileSystem *fs, const string &path);
};

} // namespace duckdb
