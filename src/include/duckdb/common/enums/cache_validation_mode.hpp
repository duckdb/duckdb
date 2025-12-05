//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/cache_validation_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class CacheValidationMode : uint8_t {
	VALIDATE_ALL = 0,      // Validate all cache entries (default)
	VALIDATE_REMOTE = 1,   // Validate only remote cache entries
	NO_VALIDATION = 2      // Disable cache validation
};

} // namespace duckdb

