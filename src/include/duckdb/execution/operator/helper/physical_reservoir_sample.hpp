//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_reservoir_sample.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_sink.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"

namespace duckdb {

//! PhysicalReservoirSample represents a sample taken using reservoir sampling, which is a blocking sampling method
class PhysicalReservoirSample : public PhysicalSink {
public:
	PhysicalReservoirSample(vector<LogicalType> types, unique_ptr<SampleOptions> options, idx_t estimated_cardinality)
	    : PhysicalSink(PhysicalOperatorType::RESERVOIR_SAMPLE, move(types), estimated_cardinality),
	      options(move(options)) {
	}

	unique_ptr<SampleOptions> options;

public:
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	string ParamsToString() const override;
};

} // namespace duckdb
