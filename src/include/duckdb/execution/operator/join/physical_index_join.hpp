//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_index_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

//! PhysicalIndexJoin represents an index join between two tables
class PhysicalIndexJoin : public PhysicalComparisonJoin {
public:
	PhysicalIndexJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                  vector<JoinCondition> cond, JoinType join_type, const vector<idx_t>& left_projection_map,
	                  vector<idx_t> right_projection_map, Index *index);
	vector<idx_t> right_projection_map;
	//! The types of the keys
	vector<LogicalType> condition_types;
	//! The types of all conditions
	vector<LogicalType> build_types;
	//! Index used for join
	Index* index {};


	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
    void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input) override;
	void Finalize(ClientContext &context, unique_ptr<GlobalOperatorState> state) override;

};

} // namespace duckdb
