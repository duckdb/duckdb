//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/nested_loop_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {
class ColumnDataCollection;

struct NestedLoopJoinInner {
	static idx_t Perform(idx_t &ltuple, idx_t &rtuple, DataChunk &left_conditions, DataChunk &right_conditions,
	                     SelectionVector &lvector, SelectionVector &rvector, const vector<JoinCondition> &conditions);
};

struct NestedLoopJoinMark {
	static void Perform(DataChunk &left, ColumnDataCollection &right, bool found_match[],
	                    const vector<JoinCondition> &conditions);
};

} // namespace duckdb
