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

struct DistinctAggregateData {
public:
	DistinctAggregateData(Allocator &allocator, const vector<unique_ptr<Expression>> &aggregates, vector<idx_t> indices,
	                      ClientContext &client);
	//! The executor
	ExpressionExecutor child_executor;
	//! The payload chunk
	DataChunk payload_chunk;
	//! Indices of the distinct aggregates
	vector<idx_t> indices;
	//! The data used by the hashtables
	vector<unique_ptr<GroupedAggregateData>> grouped_aggregate_data;
	//! The hashtables
	vector<unique_ptr<RadixPartitionedHashTable>> radix_tables;
	//! The groups (arguments)
	vector<GroupingSet> grouping_sets;
	//! The global sink states of the hash tables
	vector<unique_ptr<GlobalSinkState>> radix_states;
	//! Output chunks to receive distinct data from hashtables
	vector<unique_ptr<DataChunk>> distinct_output_chunks;
	//! Mapping from aggregate index to table
	//! This indirection is used to allow two aggregates to share the same input data
	unordered_map<idx_t, idx_t> table_map;
	//! Occupied tables, not equal to indices if aggregates share input data
	vector<idx_t> table_indices;

public:
	bool IsDistinct(idx_t index) const;
	const vector<idx_t> &Indices() const;
	bool AnyDistinct() const;

private:
	//! Returns the amount of tables that are occupied
	idx_t CreateTableIndexMap(const vector<unique_ptr<Expression>> &aggregates);
};

} // namespace duckdb
