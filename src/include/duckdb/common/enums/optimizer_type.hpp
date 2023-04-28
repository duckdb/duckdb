//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/optimizer_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class OptimizerType : uint32_t {
	INVALID = 0,
	EXPRESSION_REWRITER,
	FILTER_PULLUP,
	FILTER_PUSHDOWN,
	REGEX_RANGE,
	IN_CLAUSE,
	JOIN_ORDER,
	DELIMINATOR,
	UNNEST_REWRITER,
	UNUSED_COLUMNS,
	STATISTICS_PROPAGATION,
	COMMON_SUBEXPRESSIONS,
	COMMON_AGGREGATE,
	COLUMN_LIFETIME,
	TOP_N,
	COMPRESSED_MATERIALIZATION,
	DUPLICATE_GROUPS,
	REORDER_FILTER,
	EXTENSION
};

string OptimizerTypeToString(OptimizerType type);
OptimizerType OptimizerTypeFromString(const string &str);

} // namespace duckdb
