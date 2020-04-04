//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/relation_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Catalog Types
//===--------------------------------------------------------------------===//
enum class RelationType : uint8_t {
	INVALID,
	TABLE,
	PROJECTION,
	FILTER,
	CROSS_PRODUCT,
	JOIN,
	SET_OPERATION,
	DISTINCT,
	LIMIT,
	ORDER,
	AGGREGATE
};

} // namespace duckdb
