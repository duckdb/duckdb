//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/set_scope.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class SetScope : uint8_t {
	AUTOMATIC = 0,
	LOCAL = 1, /* unused */
	SESSION = 2,
	GLOBAL = 3,
	VARIABLE = 4
};

} // namespace duckdb
