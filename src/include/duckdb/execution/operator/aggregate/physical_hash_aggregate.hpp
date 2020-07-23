//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/physical_sink.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

//! PhysicalHashAggregate is an group-by and aggregate implementation that uses
//! a hash table to perform the grouping
class PhysicalHashAggregate : public PhysicalSink {
public:
	PhysicalHashAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
	                      PhysicalOperatorType type = PhysicalOperatorType::HASH_GROUP_BY);
	PhysicalHashAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
	                      vector<unique_ptr<Expression>> groups,
	                      PhysicalOperatorType type = PhysicalOperatorType::HASH_GROUP_BY);

	//! The groups
	vector<unique_ptr<Expression>> groups;
	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
	//! Whether or not the aggregate is an implicit (i.e. ungrouped) aggregate
	bool is_implicit_aggr;
	//! Whether or not all aggregates are combinable
	bool all_combinable;

	//! The group types
	vector<TypeId> group_types;
	//! The payload types
	vector<TypeId> payload_types;
	//! The aggregate return types
	vector<TypeId> aggregate_types;

	//! Pointers to the aggregates
	vector<BoundAggregateExpression *> bindings;

public:
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input) override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) override;
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

} // namespace duckdb
