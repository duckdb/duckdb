//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/cache_validation_mode.hpp"
#include "duckdb/common/open_file_info.hpp"

namespace duckdb {

class DatabaseInstance;

//! Get the cache validation mode from the given OpenFileInfo.
//! Returns whether the mode was explicitly set in options.
DUCKDB_API bool GetCacheValidationMode(const OpenFileInfo &info, CacheValidationMode &mode);

//! Get the cache validation mode from [OpenFileInfo options or database config.
DUCKDB_API CacheValidationMode GetCacheValidationMode(const OpenFileInfo &info, DatabaseInstance &db);

} // namespace duckdb
