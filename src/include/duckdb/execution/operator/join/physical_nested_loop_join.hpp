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

//! PhysicalNestedLoopJoin represents a nested loop join between two tables
class PhysicalNestedLoopJoin : public PhysicalComparisonJoin {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::NESTED_LOOP_JOIN;

public:
	PhysicalNestedLoopJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                       vector<JoinCondition> cond, JoinType join_type, idx_t estimated_cardinality);

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}

protected:
	// CachingOperator Interface
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
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

	static bool IsSupported(const vector<JoinCondition> &conditions, JoinType join_type);

public:
	//! Returns a list of the types of the join conditions
	vector<LogicalType> GetJoinTypes() const;

private:
	// resolve joins that output max N elements (SEMI, ANTI, MARK)
	void ResolveSimpleJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &state) const;
	// resolve joins that can potentially output N*M elements (INNER, LEFT, FULL)
	OperatorResultType ResolveComplexJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                      OperatorState &state) const;
};

} // namespace duckdb
