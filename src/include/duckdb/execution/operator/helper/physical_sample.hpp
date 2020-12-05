//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_sample.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_sink.hpp"

namespace duckdb {

//! PhysicalSample represents the SAMPLE operator
class PhysicalSample : public PhysicalSink {
public:
	PhysicalSample(vector<LogicalType> types, idx_t sample_count)
	    : PhysicalSink(PhysicalOperatorType::PHYSICAL_SAMPLE, move(types)), sample_count(sample_count) {
	}

	idx_t sample_count;

public:
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input) override;
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	string ParamsToString() const override;
};

} // namespace duckdb
