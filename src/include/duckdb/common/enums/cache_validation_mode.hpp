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
	// Validate all cache entries, default option.
	VALIDATE_ALL = 0,
	// Validate only remote cache entries.
	VALIDATE_REMOTE = 1,
	// Disable cache validation.
	NO_VALIDATION = 2,
};

} // namespace duckdb
