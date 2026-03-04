//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/cache_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace duckdb {

//! Caching mode for CachingFileSystemWrapper.
//! By default only remote files will be cached, but it's also allowed to cache local for direct IO use case.
enum class CachingMode : uint8_t {
	// Cache all files.
	ALWAYS_CACHE = 0,
	// Only cache remote files, bypass cache for local files.
	CACHE_REMOTE_ONLY = 1,
	// Doesn't perform caching.
	NO_CACHING = 2,
};

} // namespace duckdb
