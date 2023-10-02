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
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::BLOCKWISE_NL_JOIN;

public:
	PhysicalBlockwiseNLJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                        unique_ptr<Expression> condition, JoinType join_type, idx_t estimated_cardinality);

	unique_ptr<Expression> condition;

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}

protected:
	// CachingOperatorState Interface
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

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
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

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
