//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_sink.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

class ClientContext;
class BufferManager;

//! PhysicalHashAggregate is an group-by and aggregate implementation that uses
//! a hash table to perform the grouping
class PhysicalHashAggregate : public PhysicalSink {
public:
	PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
	                      idx_t estimated_cardinality, PhysicalOperatorType type = PhysicalOperatorType::HASH_GROUP_BY);
	PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
	                      vector<unique_ptr<Expression>> groups, idx_t estimated_cardinality,
	                      PhysicalOperatorType type = PhysicalOperatorType::HASH_GROUP_BY);

	//! The groups
	vector<unique_ptr<Expression>> groups;
	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
	//! Whether or not the aggregate is an implicit (i.e. ungrouped) aggregate
	bool is_implicit_aggr;
	//! Whether or not all aggregates are combinable
	bool all_combinable;

	//! Whether or not any aggregation is DISTINCT
	bool any_distinct;

	//! The group types
	vector<LogicalType> group_types;
	//! The payload types
	vector<LogicalType> payload_types;
	//! The aggregate return types
	vector<LogicalType> aggregate_return_types;

	//! Pointers to the aggregates
	vector<BoundAggregateExpression *> bindings;

	unordered_map<Expression *, size_t> filter_indexes;

public:
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input) override;
	void Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate) override;
	bool Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> gstate) override;

	void FinalizeImmediate(ClientContext &context, unique_ptr<GlobalOperatorState> gstate);

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) override;
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	string ParamsToString() const override;

private:
	//! how many groups can we have in the operator before we switch to radix partitioning
	idx_t radix_limit;

private:
	bool FinalizeInternal(ClientContext &context, unique_ptr<GlobalOperatorState> gstate, bool immediate,
	                      Pipeline *pipeline);
	bool ForceSingleHT(GlobalOperatorState &state);
};

} // namespace duckdb
