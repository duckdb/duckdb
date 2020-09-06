//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/logical_operator_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Logical Operator Types
//===--------------------------------------------------------------------===//
enum class LogicalOperatorType : uint8_t {
	INVALID = 0,
	PROJECTION = 1,
	FILTER = 2,
	AGGREGATE_AND_GROUP_BY = 3,
	WINDOW = 4,
	UNNEST = 5,
	LIMIT = 6,
	ORDER_BY = 7,
	TOP_N = 8,
	COPY_FROM_FILE = 9,
	COPY_TO_FILE = 10,
	DISTINCT = 11,
	INDEX_SCAN = 12,
	// -----------------------------
	// Data sources
	// -----------------------------
	GET = 25,
	CHUNK_GET = 26,
	DELIM_GET = 27,
	EXPRESSION_GET = 28,
	DUMMY_SCAN = 29,
	EMPTY_RESULT = 30,
	CTE_REF = 31,
	// -----------------------------
	// Joins
	// -----------------------------
	JOIN = 50,
	DELIM_JOIN = 51,
	COMPARISON_JOIN = 52,
	ANY_JOIN = 53,
	CROSS_PRODUCT = 54,
	// -----------------------------
	// SetOps
	// -----------------------------
	UNION = 75,
	EXCEPT = 76,
	INTERSECT = 77,
	RECURSIVE_CTE = 78,

	// -----------------------------
	// Updates
	// -----------------------------
	INSERT = 100,
	DELETE = 101,
	UPDATE = 102,

	// -----------------------------
	// Schema
	// -----------------------------
	ALTER = 125,
	CREATE_TABLE = 126,
	CREATE_INDEX = 127,
	CREATE_SEQUENCE = 128,
	CREATE_VIEW = 129,
	CREATE_SCHEMA = 130,
	DROP = 131,
	PRAGMA = 132,
	TRANSACTION = 133,

	// -----------------------------
	// Explain
	// -----------------------------
	EXPLAIN = 150,

	// -----------------------------
	// Helpers
	// -----------------------------
	PREPARE = 175,
	EXECUTE = 176,
	EXPORT = 177,
	VACUUM = 178
};

string LogicalOperatorToString(LogicalOperatorType type);

} // namespace duckdb
