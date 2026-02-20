//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/optimizer_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

enum class OptimizerType : uint32_t {
	INVALID = 0,
	EXPRESSION_REWRITER = 1,
	FILTER_PULLUP = 2,
	FILTER_PUSHDOWN = 3,
	EMPTY_RESULT_PULLUP = 4,
	CTE_FILTER_PUSHER = 5,
	REGEX_RANGE = 6,
	IN_CLAUSE = 7,
	JOIN_ORDER = 8,
	DELIMINATOR = 9,
	UNNEST_REWRITER = 10,
	UNUSED_COLUMNS = 11,
	STATISTICS_PROPAGATION = 12,
	COMMON_SUBEXPRESSIONS = 13,
	COMMON_AGGREGATE = 14,
	COLUMN_LIFETIME = 15,
	BUILD_SIDE_PROBE_SIDE = 16,
	LIMIT_PUSHDOWN = 17,
	TOP_N = 18,
	COMPRESSED_MATERIALIZATION = 19,
	DUPLICATE_GROUPS = 20,
	REORDER_FILTER = 21,
	SAMPLING_PUSHDOWN = 22,
	JOIN_FILTER_PUSHDOWN = 23,
	EXTENSION = 24,
	MATERIALIZED_CTE = 25,
	SUM_REWRITER = 26,
	LATE_MATERIALIZATION = 27,
	CTE_INLINING = 28,
	ROW_GROUP_PRUNER = 29,
	TOP_N_WINDOW_ELIMINATION = 30,
	COMMON_SUBPLAN = 31,
	JOIN_ELIMINATION = 32,
	WINDOW_SELF_JOIN = 33,
	PROJECTION_PULLUP = 34
};

string OptimizerTypeToString(OptimizerType type);
OptimizerType OptimizerTypeFromString(const string &str);
vector<string> ListAllOptimizers();

} // namespace duckdb
