//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/enums/logical_operator_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Logical Operator Types
//===--------------------------------------------------------------------===//
enum class LogicalOperatorType : uint8_t {
	INVALID,
	PROJECTION,
	FILTER,
	AGGREGATE_AND_GROUP_BY,
	WINDOW,
	LIMIT,
	ORDER_BY,
	COPY_FROM_FILE,
	COPY_TO_FILE,
	DISTINCT,
	INDEX_SCAN,
	// -----------------------------
	// Data sources
	// -----------------------------
	GET,
	CHUNK_GET,
	DELIM_GET,
	EXPRESSION_GET,
	TABLE_FUNCTION,
	SUBQUERY,
	EMPTY_RESULT,
	// -----------------------------
	// Joins
	// -----------------------------
	JOIN,
	DELIM_JOIN,
	COMPARISON_JOIN,
	ANY_JOIN,
	CROSS_PRODUCT,
	// -----------------------------
	// SetOps
	// -----------------------------
	UNION,
	EXCEPT,
	INTERSECT,

	// -----------------------------
	// Updates
	// -----------------------------
	INSERT,
	DELETE,
	UPDATE,
	CREATE_TABLE,
	CREATE_INDEX,

	// -----------------------------
	// Explain
	// -----------------------------
	EXPLAIN,

	// -----------------------------
	// Helpers
	// -----------------------------
	PRUNE_COLUMNS,
	PREPARE,
	EXECUTE
};

string LogicalOperatorToString(LogicalOperatorType type);

} // namespace duckdb
