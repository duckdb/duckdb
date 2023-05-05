//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/grouped_aggregate_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/aggregate/grouped_aggregate_data.hpp"
#include "duckdb/execution/radix_partitioned_hashtable.hpp"

namespace duckdb {

class GroupedAggregateData;

struct DistinctAggregateCollectionInfo {
public:
	DistinctAggregateCollectionInfo(const vector<unique_ptr<Expression>> &aggregates, vector<idx_t> indices);

public:
	// The indices of the aggregates that are distinct
	unsafe_vector<idx_t> indices;
	// The amount of radix_tables that are occupied
	idx_t table_count;
	//! Occupied tables, not equal to indices if aggregates share input data
	vector<idx_t> table_indices;
	//! This indirection is used to allow two aggregates to share the same input data
	unordered_map<idx_t, idx_t> table_map;
	const vector<unique_ptr<Expression>> &aggregates;
	// Total amount of children of the distinct aggregates
	idx_t total_child_count;

public:
	static unique_ptr<DistinctAggregateCollectionInfo> Create(vector<unique_ptr<Expression>> &aggregates);
	const unsafe_vector<idx_t> &Indices() const;
	bool AnyDistinct() const;

private:
	//! Returns the amount of tables that are occupied
	idx_t CreateTableIndexMap();
};

struct DistinctAggregateData {
public:
	DistinctAggregateData(const DistinctAggregateCollectionInfo &info);
	DistinctAggregateData(const DistinctAggregateCollectionInfo &info, const GroupingSet &groups,
	                      const vector<unique_ptr<Expression>> *group_expressions);
	//! The data used by the hashtables
	vector<unique_ptr<GroupedAggregateData>> grouped_aggregate_data;
	//! The hashtables
	vector<unique_ptr<RadixPartitionedHashTable>> radix_tables;
	//! The groups (arguments)
	vector<GroupingSet> grouping_sets;
	const DistinctAggregateCollectionInfo &info;

public:
	bool IsDistinct(idx_t index) const;
};

struct DistinctAggregateState {
public:
	DistinctAggregateState(const DistinctAggregateData &data, ClientContext &client);

	//! The executor
	ExpressionExecutor child_executor;
	//! The global sink states of the hash tables
	vector<unique_ptr<GlobalSinkState>> radix_states;
	//! Output chunks to receive distinct data from hashtables
	vector<unique_ptr<DataChunk>> distinct_output_chunks;
};

} // namespace duckdb
