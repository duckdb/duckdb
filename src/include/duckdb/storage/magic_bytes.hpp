//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/magic_bytes.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class FileSystem;
class QueryContext;
struct PrefetchedFileData;

enum class DataFileType : uint8_t {
	FILE_DOES_NOT_EXIST, // file does not exist
	DUCKDB_FILE,         // duckdb database file
	SQLITE_FILE,         // sqlite database file
	PARQUET_FILE,        // parquet file
	UNKNOWN_FILE         // unknown file type
};

class MagicBytes {
public:
	//! Detect the file type at `path` by reading its magic bytes. When `out_prefetch` is set and the file is a
	//! DuckDB database, the opened handle and a prefetched header prefix are returned through it, so a later
	//! storage open can reuse them instead of issuing a second open and re-reading the header.
	static DataFileType CheckMagicBytes(QueryContext context, FileSystem &fs, const string &path,
	                                    optional_ptr<PrefetchedFileData> out_prefetch = nullptr);
};

} // namespace duckdb
