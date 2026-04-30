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
class BufferedFileHandle;
class FileSystem;
class QueryContext;

enum class DataFileType : uint8_t {
	FILE_DOES_NOT_EXIST, // file does not exist
	DUCKDB_FILE,         // duckdb database file
	SQLITE_FILE,         // sqlite database file
	PARQUET_FILE,        // parquet file
	UNKNOWN_FILE         // unknown file type
};

class MagicBytes {
public:
	//! Check the magic bytes of the file at `path`. If `out_handle` is non-null,
	//! returns through it an open BufferedFileHandle with the duckdb header
	//! range (~12 KiB) prefetched, so a downstream storage open can reuse the
	//! handle plus its buffered prefix instead of issuing a second open + reads.
	static DataFileType CheckMagicBytes(QueryContext context, FileSystem &fs, const string &path,
	                                    unique_ptr<BufferedFileHandle> *out_handle = nullptr);
};

} // namespace duckdb
