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

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

	bool RequiresCache() const override {
		return true;
	}

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

	bool IsSource() const override {
		return IsRightOuterJoin(join_type);
	}
	bool ParallelSource() const override {
		return true;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                    DataChunk &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          GlobalSinkState &gstate) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

public:
	string ParamsToString() const override;
};

} // namespace duckdb
