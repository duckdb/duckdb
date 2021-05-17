//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_piecewise_merge_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/merge_join.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"

namespace duckdb {

//! PhysicalPiecewiseMergeJoin represents a piecewise merge loop join between
//! two tables
class PhysicalPiecewiseMergeJoin : public PhysicalComparisonJoin {
public:
	PhysicalPiecewiseMergeJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
	                           unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
	                           idx_t estimated_cardinality);

	vector<LogicalType> join_key_types;

public:
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) override;
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;
	bool Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	void FinalizeOperatorState(PhysicalOperatorState &state, ExecutionContext &context) override;
	void Combine(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate) override;

private:
	// resolve joins that output max N elements (SEMI, ANTI, MARK)
	void ResolveSimpleJoin(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const;
	// resolve joins that can potentially output N*M elements (INNER, LEFT, FULL)
	void ResolveComplexJoin(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const;
};

} // namespace duckdb
