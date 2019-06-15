//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/enums/physical_operator_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Physical Operator Types
//===--------------------------------------------------------------------===//
enum class PhysicalOperatorType : uint8_t {
	INVALID,
	LEAF,
	ORDER_BY,
	LIMIT,
	AGGREGATE,
	WINDOW,
	DISTINCT,
	HASH_GROUP_BY,
	SORT_GROUP_BY,
	FILTER,
	PROJECTION,
	BASE_GROUP_BY,
	COPY_FROM_FILE,
	COPY_TO_FILE,
	TABLE_FUNCTION,
	// -----------------------------
	// Scans
	// -----------------------------
	DUMMY_SCAN,
	SEQ_SCAN,
	INDEX_SCAN,
	CHUNK_SCAN,
	DELIM_SCAN,
	EXTERNAL_FILE_SCAN,
	QUERY_DERIVED_SCAN,
	EXPRESSION_SCAN,
	// -----------------------------
	// Joins
	// -----------------------------
	BLOCKWISE_NL_JOIN,
	NESTED_LOOP_JOIN,
	HASH_JOIN,
	CROSS_PRODUCT,
	PIECEWISE_MERGE_JOIN,
	DELIM_JOIN,

	// -----------------------------
	// SetOps
	// -----------------------------
	UNION,

	// -----------------------------
	// Updates
	// -----------------------------
	INSERT,
	INSERT_SELECT,
	DELETE,
	UPDATE,
	EXPORT_EXTERNAL_FILE,
	CREATE,
	CREATE_INDEX,
	// -----------------------------
	// Helpers
	// -----------------------------
	PRUNE_COLUMNS,
	EXPLAIN,
	EMPTY_RESULT,
	EXECUTE
};

string PhysicalOperatorToString(PhysicalOperatorType type);

} // namespace duckdb
