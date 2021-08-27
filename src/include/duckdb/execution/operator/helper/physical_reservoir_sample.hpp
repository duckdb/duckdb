//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_reservoir_sample.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"

namespace duckdb {

//! PhysicalReservoirSample represents a sample taken using reservoir sampling, which is a blocking sampling method
class PhysicalReservoirSample : public PhysicalOperator {
public:
	PhysicalReservoirSample(vector<LogicalType> types, unique_ptr<SampleOptions> options, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::RESERVOIR_SAMPLE, move(types), estimated_cardinality),
	      options(move(options)) {
	}

	unique_ptr<SampleOptions> options;

public:
	// Source interface
	virtual void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate, LocalSourceState &lstate) const;

public:
	// Sink interface
	void Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	string ParamsToString() const override;
};

} // namespace duckdb
