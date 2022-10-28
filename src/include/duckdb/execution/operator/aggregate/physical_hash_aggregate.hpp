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
#include "duckdb/execution/operator/aggregate/distinct_aggregate_data.hpp"

namespace duckdb {

class ClientContext;
class BufferManager;

struct HashAggregateGroupingData {
public:
	HashAggregateGroupingData(GroupingSet &grouping_set_p, const GroupedAggregateData &grouped_aggregate_data,
	                          unique_ptr<DistinctAggregateCollectionInfo> &info);

public:
	RadixPartitionedHashTable table_data;
	unique_ptr<DistinctAggregateData> distinct_data;

public:
	bool HasDistinct() const;
};

struct HashAggregateGroupingGlobalState {
public:
	HashAggregateGroupingGlobalState(const HashAggregateGroupingData &data, ClientContext &context);
	// Radix state of the GROUPING_SET ht
	unique_ptr<GlobalSinkState> table_state;
	// State of the DISTINCT aggregates of this GROUPING_SET
	unique_ptr<DistinctAggregateState> distinct_state;
};

struct HashAggregateGroupingLocalState {
public:
	HashAggregateGroupingLocalState(const PhysicalHashAggregate &op, const HashAggregateGroupingData &data,
	                                ExecutionContext &context);

public:
	// Radix state of the GROUPING_SET ht
	unique_ptr<LocalSinkState> table_state;
	// Local states of the DISTINCT aggregates hashtables
	vector<unique_ptr<LocalSinkState>> distinct_states;
};

//! PhysicalHashAggregate is a group-by and aggregate implementation that uses a hash table to perform the grouping
//! This only contains read-only variables, anything that is stateful instead gets stored in the Global/Local states
class PhysicalHashAggregate : public PhysicalOperator {
public:
	PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
	                      idx_t estimated_cardinality);
	PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
	                      vector<unique_ptr<Expression>> groups, idx_t estimated_cardinality);
	PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
	                      vector<unique_ptr<Expression>> groups, vector<GroupingSet> grouping_sets,
	                      vector<vector<idx_t>> grouping_functions, idx_t estimated_cardinality);

	//! The grouping sets
	GroupedAggregateData grouped_aggregate_data;

	vector<GroupingSet> grouping_sets;
	//! The radix partitioned hash tables (one per grouping set)
	vector<HashAggregateGroupingData> groupings;
	unique_ptr<DistinctAggregateCollectionInfo> distinct_collection_info;
	//! A recreation of the input chunk, with nulls for everything that isnt a group
	vector<LogicalType> input_group_types;

	// Filters given to Sink and friends
	vector<idx_t> non_distinct_filter;
	vector<idx_t> distinct_filter;

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
	SinkFinalizeType FinalizeInternal(Pipeline &pipeline, Event &event, ClientContext &context, GlobalSinkState &gstate,
	                                  bool check_distinct) const;

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

private:
	//! Finalize the distinct aggregates
	SinkFinalizeType FinalizeDistinct(Pipeline &pipeline, Event &event, ClientContext &context,
	                                  GlobalSinkState &gstate) const;
	//! Combine the distinct aggregates
	void CombineDistinct(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const;
	//! Sink the distinct aggregates for a single grouping
	void SinkDistinctGrouping(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                          DataChunk &input, idx_t grouping_idx) const;
	//! Sink the distinct aggregates
	void SinkDistinct(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                  DataChunk &input) const;
};

} // namespace duckdb
