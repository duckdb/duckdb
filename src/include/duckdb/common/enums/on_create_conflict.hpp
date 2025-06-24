//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/on_create_conflict.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class OnCreateConflict : uint8_t {
	// Standard: throw error
	ERROR_ON_CONFLICT,
	// CREATE IF NOT EXISTS, silently do nothing on conflict
	IGNORE_ON_CONFLICT,
	// CREATE OR REPLACE
	REPLACE_ON_CONFLICT,
	// Update on conflict - only support for functions. Add a function overload if the function already exists.
	ALTER_ON_CONFLICT
};

} // namespace duckdb
