//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_asof_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

namespace duckdb {

//! PhysicalAsOfJoin represents an as-of join between two tables
class PhysicalAsOfJoin : public PhysicalComparisonJoin {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::ASOF_JOIN;

public:
	PhysicalAsOfJoin(PhysicalPlan &physical_plan, LogicalComparisonJoin &op, PhysicalOperator &left,
	                 PhysicalOperator &right);

	vector<LogicalType> join_key_types;
	vector<column_t> null_sensitive;
	ExpressionType comparison_type;

	// Equalities
	vector<unique_ptr<Expression>> lhs_partitions;
	vector<unique_ptr<Expression>> rhs_partitions;

	// Inequality Only
	vector<BoundOrderByNode> lhs_orders;
	vector<BoundOrderByNode> rhs_orders;

	// Projection mappings
	vector<column_t> right_projection_map;

protected:
	// CachingOperator Interface
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

public:
	// Source interface
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
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

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
};

} // namespace duckdb
