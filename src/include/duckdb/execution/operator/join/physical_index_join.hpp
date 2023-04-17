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
#include "duckdb/storage/index.hpp"

namespace duckdb {

//! PhysicalIndexJoin represents an index join between two tables
class PhysicalIndexJoin : public CachingPhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::INDEX_JOIN;

public:
	PhysicalIndexJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                  vector<JoinCondition> cond, JoinType join_type, const vector<idx_t> &left_projection_map,
	                  vector<idx_t> right_projection_map, vector<column_t> column_ids, Index &index, bool lhs_first,
	                  idx_t estimated_cardinality);

	//! Columns from RHS used in the query
	vector<column_t> column_ids;
	//! Columns to be fetched
	vector<column_t> fetch_ids;
	//! Types of fetch columns
	vector<LogicalType> fetch_types;
	//! Columns indexed by index
	unordered_set<column_t> index_ids;
	//! Projected ids from LHS
	vector<column_t> left_projection_map;
	//! Projected ids from RHS
	vector<column_t> right_projection_map;
	//! The types of the keys
	vector<LogicalType> condition_types;
	//! The types of all conditions
	vector<LogicalType> build_types;
	//! Index used for join
	Index &index;

	vector<JoinCondition> conditions;

	JoinType join_type;
	//! In case we swap rhs with lhs we need to output columns related to rhs first.
	bool lhs_first = true;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	OrderPreservationType OperatorOrder() const override {
		return OrderPreservationType::NO_ORDER;
	}
	bool ParallelOperator() const override {
		return true;
	}

protected:
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	vector<const_reference<PhysicalOperator>> GetSources() const override;

private:
	void GetRHSMatches(ExecutionContext &context, DataChunk &input, OperatorState &state_p) const;
	//! Fills result chunk
	void Output(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &state_p) const;
};

} // namespace duckdb
