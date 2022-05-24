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
	LOGICAL_INVALID = 0,
	LOGICAL_PROJECTION = 1,
	LOGICAL_FILTER = 2,
	LOGICAL_AGGREGATE_AND_GROUP_BY = 3,
	LOGICAL_WINDOW = 4,
	LOGICAL_UNNEST = 5,
	LOGICAL_LIMIT = 6,
	LOGICAL_ORDER_BY = 7,
	LOGICAL_TOP_N = 8,
	LOGICAL_COPY_TO_FILE = 10,
	LOGICAL_DISTINCT = 11,
	LOGICAL_SAMPLE = 12,
	LOGICAL_LIMIT_PERCENT = 13,

	// -----------------------------
	// Data sources
	// -----------------------------
	LOGICAL_GET = 25,
	LOGICAL_CHUNK_GET = 26,
	LOGICAL_DELIM_GET = 27,
	LOGICAL_EXPRESSION_GET = 28,
	LOGICAL_DUMMY_SCAN = 29,
	LOGICAL_EMPTY_RESULT = 30,
	LOGICAL_CTE_REF = 31,
	// -----------------------------
	// Joins
	// -----------------------------
	LOGICAL_JOIN = 50,
	LOGICAL_DELIM_JOIN = 51,
	LOGICAL_COMPARISON_JOIN = 52,
	LOGICAL_ANY_JOIN = 53,
	LOGICAL_CROSS_PRODUCT = 54,
	// -----------------------------
	// SetOps
	// -----------------------------
	LOGICAL_UNION = 75,
	LOGICAL_EXCEPT = 76,
	LOGICAL_INTERSECT = 77,
	LOGICAL_RECURSIVE_CTE = 78,

	// -----------------------------
	// Updates
	// -----------------------------
	LOGICAL_INSERT = 100,
	LOGICAL_DELETE = 101,
	LOGICAL_UPDATE = 102,

	// -----------------------------
	// Schema
	// -----------------------------
	LOGICAL_ALTER = 125,
	LOGICAL_CREATE_TABLE = 126,
	LOGICAL_CREATE_INDEX = 127,
	LOGICAL_CREATE_SEQUENCE = 128,
	LOGICAL_CREATE_VIEW = 129,
    LOGICAL_CREATE_MATVIEW = 130,
	LOGICAL_CREATE_SCHEMA = 131,
	LOGICAL_CREATE_MACRO = 132,
	LOGICAL_DROP = 133,
	LOGICAL_PRAGMA = 134,
	LOGICAL_TRANSACTION = 135,
	LOGICAL_CREATE_TYPE = 136,

	// -----------------------------
	// Explain
	// -----------------------------
	LOGICAL_EXPLAIN = 150,

	// -----------------------------
	// Show
	// -----------------------------
	LOGICAL_SHOW = 160,

	// -----------------------------
	// Helpers
	// -----------------------------
	LOGICAL_PREPARE = 175,
	LOGICAL_EXECUTE = 176,
	LOGICAL_EXPORT = 177,
	LOGICAL_VACUUM = 178,
	LOGICAL_SET = 179,
	LOGICAL_LOAD = 180
};

DUCKDB_API string LogicalOperatorToString(LogicalOperatorType type);

} // namespace duckdb
