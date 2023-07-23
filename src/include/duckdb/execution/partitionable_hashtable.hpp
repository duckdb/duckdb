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

struct RadixPartitionInfo {
	explicit RadixPartitionInfo(idx_t n_partitions_upper_bound);
	const idx_t n_partitions;
	const idx_t radix_bits;
	const hash_t radix_mask;
	const idx_t radix_shift;

	inline hash_t GetHashPartition(hash_t hash) const {
		return (hash & radix_mask) >> radix_shift;
	}
};

typedef vector<unique_ptr<GroupedAggregateHashTable>> HashTableList; // NOLINT

class PartitionableHashTable {
public:
	PartitionableHashTable(ClientContext &context, Allocator &allocator, RadixPartitionInfo &partition_info_p,
	                       vector<LogicalType> group_types_p, vector<LogicalType> payload_types_p,
	                       vector<BoundAggregateExpression *> bindings_p);

	idx_t AddChunk(DataChunk &groups, DataChunk &payload, bool do_partition, const unsafe_vector<idx_t> &filter);
	void Partition(bool sink_done);
	bool IsPartitioned();

	HashTableList GetPartition(idx_t partition);
	HashTableList GetUnpartitioned();
	idx_t GetPartitionCount(idx_t partition) const;
	idx_t GetPartitionSize(idx_t partition) const;

	void Finalize();

	void Append(GroupedAggregateHashTable &ht);

private:
	ClientContext &context;
	Allocator &allocator;
	vector<LogicalType> group_types;
	vector<LogicalType> payload_types;
	vector<BoundAggregateExpression *> bindings;

	bool is_partitioned;
	RadixPartitionInfo &partition_info;
	vector<SelectionVector> sel_vectors;
	unsafe_vector<idx_t> sel_vector_sizes;
	DataChunk group_subset, payload_subset;
	Vector hashes, hashes_subset;
	AggregateHTAppendState append_state;

	HashTableList unpartitioned_hts;
	vector<HashTableList> radix_partitioned_hts;
	idx_t tuple_size;

private:
	idx_t ListAddChunk(HashTableList &list, DataChunk &groups, Vector &group_hashes, DataChunk &payload,
	                   const unsafe_vector<idx_t> &filter);
	//! Returns the HT entry size used for intermediate hash tables
	HtEntryType GetHTEntrySize();
};
} // namespace duckdb
