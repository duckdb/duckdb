//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database_path_and_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include "duckdb/main/config.hpp"

namespace duckdb {

class BufferedFileHandle;
class QueryContext;

struct DBPathAndType {
	//! Parse database extension type and rest of path from combined form (type:path)
	static void ExtractExtensionPrefix(string &path, string &db_type);
	//! Check the magic bytes of a file and set the database type based on that.
	//! If `out_handle` is non-null, returns through it an open BufferedFileHandle
	//! with the duckdb header range prefetched (for downstream storage open).
	static void CheckMagicBytes(QueryContext context, FileSystem &fs, string &path, string &db_type,
	                            unique_ptr<BufferedFileHandle> *out_handle = nullptr);

	//! Run ExtractExtensionPrefix followed by CheckMagicBytes.
	//! Same `out_handle` semantics as CheckMagicBytes.
	static void ResolveDatabaseType(FileSystem &fs, string &path, string &db_type,
	                                unique_ptr<BufferedFileHandle> *out_handle = nullptr);
};
} // namespace duckdb
