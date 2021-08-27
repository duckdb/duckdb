//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_streaming_sample.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"

namespace duckdb {

//! PhysicalStreamingSample represents a streaming sample using either system or bernoulli sampling
class PhysicalStreamingSample : public PhysicalOperator {
public:
	PhysicalStreamingSample(vector<LogicalType> types, SampleMethod method, double percentage, int64_t seed,
	                        idx_t estimated_cardinality);

	SampleMethod method;
	double percentage;
	int64_t seed;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, OperatorState *state) const override;
	unique_ptr<OperatorState> GetOperatorState() override;

	string ParamsToString() const override;

private:
	void SystemSample(DataChunk &input, DataChunk &result, OperatorState *state) const;
	void BernoulliSample(DataChunk &input, DataChunk &result, OperatorState *state) const;
};

} // namespace duckdb
