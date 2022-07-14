//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/execution/radix_partitioned_hashtable.hpp"

namespace duckdb {

class ClientContext;
class BufferManager;

//! PhysicalHashAggregate is an group-by and aggregate implementation that uses
//! a hash table to perform the grouping
class PhysicalHashAggregate : public PhysicalOperator {
public:
	PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
	                      idx_t estimated_cardinality, PhysicalOperatorType type = PhysicalOperatorType::HASH_GROUP_BY);
	PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
	                      vector<unique_ptr<Expression>> groups, idx_t estimated_cardinality,
	                      PhysicalOperatorType type = PhysicalOperatorType::HASH_GROUP_BY);
	PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
	                      vector<unique_ptr<Expression>> groups, vector<GroupingSet> grouping_sets,
	                      vector<vector<idx_t>> grouping_functions, idx_t estimated_cardinality,
	                      PhysicalOperatorType type = PhysicalOperatorType::HASH_GROUP_BY);

	//! The groups
	vector<unique_ptr<Expression>> groups;
	//! The grouping sets
	vector<GroupingSet> grouping_sets;
	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
	//! The set of GROUPING functions
	vector<vector<idx_t>> grouping_functions;

	//! Whether or not any aggregation is DISTINCT
	bool any_distinct;

	//! The group types
	vector<LogicalType> group_types;
	//! The payload types
	vector<LogicalType> payload_types;
	//! The aggregate return types
	vector<LogicalType> aggregate_return_types;

	//! The radix partitioned hash tables (one per grouping set)
	vector<RadixPartitionedHashTable> radix_tables;

	//! Pointers to the aggregates
	vector<BoundAggregateExpression *> bindings;

	unordered_map<Expression *, size_t> filter_indexes;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

	bool ParallelSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                    DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          GlobalSinkState &gstate) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

public:
	string ParamsToString() const override;
	//! Toggle multi-scan capability on a hash table, which prevents the scan of the aggregate from being destructive
	//! If this is not toggled the GetData method will destroy the hash table as it is scanning it
	static void SetMultiScan(GlobalSinkState &state);
};

} // namespace duckdb
