//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_sink.hpp"
#include "duckdb/execution/base_aggregate_hashtable.hpp"

namespace duckdb {
class ClientContext;
class PerfectAggregateHashTable;

//! PhysicalPerfectHashAggregate performs a group-by and aggregation using a perfect hash table
class PhysicalPerfectHashAggregate : public PhysicalSink {
public:
	PhysicalPerfectHashAggregate(ClientContext &context, vector<LogicalType> types,
	                             vector<unique_ptr<Expression>> aggregates, vector<unique_ptr<Expression>> groups,
	                             vector<unique_ptr<BaseStatistics>> group_stats, vector<idx_t> required_bits,
	                             idx_t estimated_cardinality);

	//! The groups
	vector<unique_ptr<Expression>> groups;
	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;

public:
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate) override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) override;
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	string ParamsToString() const override;

	//! Create a perfect aggregate hash table for this node
	unique_ptr<PerfectAggregateHashTable> CreateHT(ClientContext &context);

public:
	//! The group types
	vector<LogicalType> group_types;
	//! The payload types
	vector<LogicalType> payload_types;
	//! The aggregates to be computed
	vector<AggregateObject> aggregate_objects;
	//! The minimum value of each of the groups
	vector<Value> group_minima;
	//! The number of bits we need to completely cover each of the groups
	vector<idx_t> required_bits;

	unordered_map<Expression *, size_t> filter_indexes;
};

} // namespace duckdb
