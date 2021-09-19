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
class PhysicalSimpleAggregate : public PhysicalOperator {
public:
	PhysicalSimpleAggregate(vector<LogicalType> types, vector<unique_ptr<Expression>> expressions, bool all_combinable,
	                        idx_t estimated_cardinality);

	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
	//! Whether or not all aggregates are trivially combinable. Aggregates that are trivially combinable can be
	//! parallelized.
	bool all_combinable;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate, LocalSourceState &lstate) const override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	string ParamsToString() const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		// we can only parallelize if all aggregates are combinable
		return all_combinable;
	}
};

} // namespace duckdb
