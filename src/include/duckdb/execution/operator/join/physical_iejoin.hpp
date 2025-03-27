//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_piecewise_merge_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/join/physical_range_join.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

namespace duckdb {

//! PhysicalIEJoin represents a two inequality range join between
//! two tables
class PhysicalIEJoin : public PhysicalRangeJoin {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::IE_JOIN;

public:
	PhysicalIEJoin(LogicalComparisonJoin &op, PhysicalOperator &left, PhysicalOperator &right,
	               vector<JoinCondition> cond, JoinType join_type, idx_t estimated_cardinality);

	vector<LogicalType> join_key_types;
	vector<BoundOrderByNode> lhs_orders;
	vector<BoundOrderByNode> rhs_orders;

public:
	// CachingOperator Interface
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

public:
	// Source interface
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}

	ProgressData GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const override;

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

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

private:
	// resolve joins that can potentially output N*M elements (INNER, LEFT, FULL)
	void ResolveComplexJoin(ExecutionContext &context, DataChunk &result, LocalSourceState &state) const;
};

} // namespace duckdb
