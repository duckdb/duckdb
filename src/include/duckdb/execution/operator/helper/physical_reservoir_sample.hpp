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
	    : PhysicalOperator(PhysicalOperatorType::RESERVOIR_SAMPLE, std::move(types), estimated_cardinality),
	      options(std::move(options)) {
	}

	unique_ptr<SampleOptions> options;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool ParallelSink() const override {
		return true;
	}

	bool IsSink() const override {
		return true;
	}

	string ParamsToString() const override;
};

} // namespace duckdb
