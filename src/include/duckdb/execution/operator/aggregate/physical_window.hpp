//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_window.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_sink.hpp"

namespace duckdb {

//! PhysicalWindow implements window functions
class PhysicalWindow : public PhysicalSink {
public:
	PhysicalWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
	               PhysicalOperatorType type = PhysicalOperatorType::WINDOW);

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	// sink stuff
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input) override;
	void Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate) override;
	void Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> gstate) override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) override;
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	string ParamsToString() const override;

public:
	//! The projection list of the WINDOW statement (may contain aggregates)
	vector<unique_ptr<Expression>> select_list;
};

} // namespace duckdb
