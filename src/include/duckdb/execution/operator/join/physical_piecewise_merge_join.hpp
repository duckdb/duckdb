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
	                           unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type);

	vector<TypeId> join_key_types;

public:
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ClientContext &context) override;
	void Sink(ClientContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input) override;
	void Finalize(ClientContext &context, unique_ptr<GlobalOperatorState> state) override;

	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
private:
    // resolve joins that output max N elements (SEMI, ANTI, MARK)
    void ResolveSimpleJoin(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state);
    // resolve joins that can potentially output N*M elements (INNER, LEFT, FULL)
    void ResolveComplexJoin(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state);
};

} // namespace duckdb
