//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_right_delim_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/join/physical_delim_join.hpp"

namespace duckdb {

//! PhysicalRightDelimJoin represents a join where the RHS will be duplicate eliminated and pushed into a
//! PhysicalColumnDataScan in the LHS.
class PhysicalRightDelimJoin : public PhysicalDelimJoin {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::RIGHT_DELIM_JOIN;

public:
	PhysicalRightDelimJoin(PhysicalPlanGenerator &planner, vector<LogicalType> types, PhysicalOperator &original_join,
	                       PhysicalOperator &distinct, const vector<const_reference<PhysicalOperator>> &delim_scans,
	                       idx_t estimated_cardinality, optional_idx delim_idx);

public:
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	void PrepareFinalize(ClientContext &context, GlobalSinkState &sink_state) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
};

} // namespace duckdb
