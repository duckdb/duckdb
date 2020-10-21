//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/partitionable_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/aggregate_hashtable.hpp"

namespace duckdb {

class PartitionableHashTable {
public:
	PartitionableHashTable(BufferManager &_buffer_manager, idx_t _radix_partitions, vector<LogicalType> _group_types,
	                       vector<LogicalType> _payload_types, vector<BoundAggregateExpression *> _bindings);

	idx_t AddChunk(DataChunk &groups, DataChunk &payload, bool do_partition);
	void Partition();
	bool IsPartitioned();

	vector<unique_ptr<GroupedAggregateHashTable>> GetPartition(idx_t partition);
	unique_ptr<GroupedAggregateHashTable> GetUnpartitioned();

private:
	BufferManager &buffer_manager;
	idx_t radix_partitions;

	vector<LogicalType> group_types;
	vector<LogicalType> payload_types;
	vector<BoundAggregateExpression *> bindings;

	// some more radix config
	idx_t radix_bits;
	hash_t radix_mask;

	constexpr static idx_t RADIX_SHIFT = 24;

	bool is_partitioned;

	vector<SelectionVector> sel_vectors;
	vector<idx_t> sel_vector_sizes;

	DataChunk group_subset, payload_subset;
	Vector hashes, hashes_subset;

	unique_ptr<GroupedAggregateHashTable> unpartitioned_ht;
	unordered_map<hash_t, vector<unique_ptr<GroupedAggregateHashTable>>> radix_partitioned_hts;
};

} // namespace duckdb
