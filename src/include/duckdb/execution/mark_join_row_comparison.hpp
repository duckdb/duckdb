//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/mark_join_row_comparison.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct MarkJoinRowComparison {
	static void CompareEquality(const Vector &left, idx_t left_row, idx_t left_count, const Vector &right,
	                            idx_t right_count, bool row_is_false[], bool row_is_unknown[]);
};

} // namespace duckdb
