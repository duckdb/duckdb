//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_left_delim_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/join/physical_delim_join.hpp"

namespace duckdb {

//! PhysicalLeftDelimJoin represents a join where the LHS will be duplicate eliminated and pushed into a
//! PhysicalColumnDataScan in the RHS.
class PhysicalLeftDelimJoin : public PhysicalDelimJoin {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::LEFT_DELIM_JOIN;

public:
	PhysicalLeftDelimJoin(vector<LogicalType> types, unique_ptr<PhysicalOperator> original_join,
	                      vector<const_reference<PhysicalOperator>> delim_scans, idx_t estimated_cardinality,
	                      optional_idx delim_idx);

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
