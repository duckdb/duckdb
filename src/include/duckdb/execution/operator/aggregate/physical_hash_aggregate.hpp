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
#include "duckdb/execution/operator/aggregate/grouped_aggregate_data.hpp"

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

	//! The grouping sets
	vector<GroupingSet> grouping_sets;
	GroupedAggregateData grouped_aggregate_data;

	//! The radix partitioned hash tables (one per grouping set)
	vector<RadixPartitionedHashTable> radix_tables;

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

	bool IsOrderPreserving() const override {
		return false;
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
