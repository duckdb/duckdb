//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/nested_loop_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/chunk_collection.hpp"
#include "common/types/vector.hpp"
#include "planner/operator/logical_comparison_join.hpp"

namespace duckdb {

struct NestedLoopJoinInner {
	static index_t Perform(index_t &ltuple, index_t &rtuple, DataChunk &left_conditions, DataChunk &right_conditions,
	                       sel_t lvector[], sel_t rvector[], vector<JoinCondition> &conditions);
};

struct NestedLoopJoinMark {
	static void Perform(DataChunk &left, ChunkCollection &right, bool found_match[], vector<JoinCondition> &conditions);
};

} // namespace duckdb
