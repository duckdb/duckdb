//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_nested_loop_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"

namespace duckdb {
idx_t nested_loop_join(ExpressionType op, Vector &left, Vector &right, idx_t &lpos, idx_t &rpos, sel_t lvector[],
                       sel_t rvector[]);
idx_t nested_loop_comparison(ExpressionType op, Vector &left, Vector &right, sel_t lvector[], sel_t rvector[],
                             idx_t count);

//! PhysicalNestedLoopJoin represents a nested loop join between two tables
class PhysicalNestedLoopJoin : public PhysicalComparisonJoin {
public:
	PhysicalNestedLoopJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                       vector<JoinCondition> cond, JoinType join_type, idx_t estimated_cardinality);

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ClientContext &context) const override;
	bool Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

public:
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	void Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const override;
	bool Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalSinkState> gstate) override;

private:
	// resolve joins that output max N elements (SEMI, ANTI, MARK)
	void ResolveSimpleJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &state) const;
	// resolve joins that can potentially output N*M elements (INNER, LEFT, FULL)
	bool ResolveComplexJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &state) const;
};

} // namespace duckdb
