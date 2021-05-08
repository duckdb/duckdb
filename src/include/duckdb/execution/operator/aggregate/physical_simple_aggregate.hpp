//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_simple_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_sink.hpp"
namespace duckdb {

//! PhysicalSimpleAggregate is an aggregate operator that can only perform aggregates (1) without any groups, and (2)
//! without any DISTINCT aggregates
class PhysicalSimpleAggregate : public PhysicalSink {
public:
	PhysicalSimpleAggregate(vector<LogicalType> types, vector<unique_ptr<Expression>> expressions, bool all_combinable,
	                        idx_t estimated_cardinality);

	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
	//! Whether or not all aggregates are trivially combinable. Aggregates that are trivially combinable can be
	//! parallelized.
	bool all_combinable;

public:
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate) override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) override;
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	string ParamsToString() const override;
};

} // namespace duckdb
