//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/stats_merge_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! How two statistics objects are being merged.
//! MERGE_STATS: both sides represent disjoint data sets — additive stats (e.g. total_string_length) are summed.
//! EXPAND_BOUNDS: one or both sides are approximate bounds — additive stats become unknown (has_X = false).
enum class StatsMergeType : uint8_t {
	MERGE_STATS = 0,
	EXPAND_BOUNDS = 1,
};

} // namespace duckdb
