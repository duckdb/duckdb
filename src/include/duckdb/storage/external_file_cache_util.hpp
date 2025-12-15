//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/cache_validation_mode.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

struct OpenFileInfo;
class DatabaseInstance;
class ClientContext;

struct ExternalFileCacheUtil {
	//! Get the cache validation mode in order of file open info, client context (for client-local settings), or
	//! database config.
	static CacheValidationMode GetCacheValidationMode(const OpenFileInfo &info,
	                                                  optional_ptr<ClientContext> client_context, DatabaseInstance &db);
};

} // namespace duckdb
