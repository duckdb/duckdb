//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/enums/index_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Index Types
//===--------------------------------------------------------------------===//
enum class IndexType {
	INVALID = 0,     // invalid index type
	ORDER_INDEX = 1, // Order Index
	BTREE = 2        // B+-Tree
};

} // namespace duckdb
