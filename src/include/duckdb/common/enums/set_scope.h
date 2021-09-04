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
	LOCAL = 0, /* Not implemented*/
	SESSION = 1,
	GLOBAL = 2
};

} // namespace duckdb
