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
	INVALID,
	PROJECTION,
	FILTER,
	AGGREGATE_AND_GROUP_BY,
	WINDOW,
	UNNEST,
	LIMIT,
	ORDER_BY,
	TOP_N,
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
	EMPTY_RESULT,
	CTE_REF,
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
	RECURSIVE_CTE,

	// -----------------------------
	// Updates
	// -----------------------------
	INSERT,
	DELETE,
	UPDATE,

	// -----------------------------
	// Schema
	// -----------------------------
	ALTER,
	CREATE_TABLE,
	CREATE_INDEX,
	CREATE_SEQUENCE,
	CREATE_VIEW,
	CREATE_SCHEMA,
	DROP,
	PRAGMA,
	TRANSACTION,

	// -----------------------------
	// Explain
	// -----------------------------
	EXPLAIN,

	// -----------------------------
	// Helpers
	// -----------------------------
	PREPARE,
	EXECUTE,
	VACUUM
};

string LogicalOperatorToString(LogicalOperatorType type);

} // namespace duckdb
