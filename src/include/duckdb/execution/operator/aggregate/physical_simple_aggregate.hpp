//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_simple_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalSimpleAggregate is an aggregate operator that can only perform aggregates (1) without any groups, and (2)
//! without any DISTINCT aggregates
class PhysicalSimpleAggregate : public PhysicalSink {
public:
	PhysicalSimpleAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions);

	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
public:
	void Sink(ClientContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input) override;
	void Combine(ClientContext &context, GlobalOperatorState &state, LocalSinkState &lstate) override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ClientContext &context) override;
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
};

} // namespace duckdb
