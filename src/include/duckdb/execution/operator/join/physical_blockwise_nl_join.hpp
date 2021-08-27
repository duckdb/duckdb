//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_blockwise_nl_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"

namespace duckdb {

//! PhysicalBlockwiseNLJoin represents a nested loop join between two tables on arbitrary expressions. This is different
//! from the PhysicalNestedLoopJoin in that it does not require expressions to be comparisons between the LHS and the
//! RHS.
class PhysicalBlockwiseNLJoin : public PhysicalJoin {
public:
	PhysicalBlockwiseNLJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                        unique_ptr<Expression> condition, JoinType join_type, idx_t estimated_cardinality);

	unique_ptr<Expression> condition;

// public:
// 	// Operator interface
// 	unique_ptr<OperatorState> GetOperatorState(ClientContext &context) const override;
// 	bool Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	void Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;
	bool Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalSinkState> state) override;

	string ParamsToString() const override;
};

} // namespace duckdb
