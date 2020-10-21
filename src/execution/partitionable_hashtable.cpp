#include "duckdb/execution/partitionable_hashtable.hpp"

namespace duckdb {

PartitionableHashTable::PartitionableHashTable(BufferManager &_buffer_manager, idx_t _radix_partitions,
                                               vector<LogicalType> _group_types, vector<LogicalType> _payload_types,
                                               vector<BoundAggregateExpression *> _bindings)
    : buffer_manager(_buffer_manager), radix_partitions(_radix_partitions), group_types(_group_types),
      payload_types(_payload_types), bindings(_bindings), radix_bits(0), radix_mask(0), is_partitioned(false) {

	unpartitioned_ht = make_unique<GroupedAggregateHashTable>(buffer_manager, group_types, payload_types, bindings,
	                                                          HtEntryType::HT_WIDTH_32);

	// finalize_threads needs to be a power of 2
	assert(radix_partitions > 0);
	assert(radix_partitions <= 256);
	assert((radix_partitions & (radix_partitions - 1)) == 0);

	auto radix_partitions_copy = radix_partitions;
	while (radix_partitions_copy - 1) {
		radix_bits++;
		radix_partitions_copy >>= 1;
	}

	assert(radix_bits <= 8);

	// we use the fifth byte of the 64 bit hash as radix source
	radix_mask = 0;
	for (idx_t i = 0; i < radix_bits; i++) {
		radix_mask = (radix_mask << 1) | 1;
	}
	radix_mask <<= RADIX_SHIFT;

	sel_vectors.resize(radix_partitions);
	sel_vector_sizes.resize(radix_partitions);
	group_subset.Initialize(group_types);
	if (payload_types.size() > 0) {
		payload_subset.Initialize(payload_types);
	}
	hashes.Initialize(LogicalType::HASH);
	hashes_subset.Initialize(LogicalType::HASH);

	for (hash_t r = 0; r < radix_partitions; r++) {
		sel_vectors[r].Initialize();
	}
}

idx_t PartitionableHashTable::AddChunk(DataChunk &groups, DataChunk &payload, bool do_partition) {
	groups.Hash(hashes);

	// we partition when we are asked to or when the unpartitioned ht runs out of space
	if (!IsPartitioned() &&
	    (do_partition || groups.size() + unpartitioned_ht->Size() > unpartitioned_ht->MaxCapacity())) {
		Partition();
	}

	if (!IsPartitioned()) {
		return unpartitioned_ht->AddChunk(groups, hashes, payload);
	}

	// makes no sense to do this with 1 partition
	assert(radix_partitions > 1);

	for (hash_t r = 0; r < radix_partitions; r++) {
		sel_vector_sizes[r] = 0;
	}

	assert(hashes.vector_type == VectorType::FLAT_VECTOR);
	auto hashes_ptr = FlatVector::GetData<hash_t>(hashes);

	for (idx_t i = 0; i < groups.size(); i++) {
		auto partition = (hashes_ptr[i] & radix_mask) >> RADIX_SHIFT;
		assert(partition < radix_partitions);
		sel_vectors[partition].set_index(sel_vector_sizes[partition]++, i);
	}

#ifdef DEBUG
	// make sure we have lost no rows
	idx_t total_count = 0;
	for (idx_t r = 0; r < radix_partitions; r++) {
		total_count += sel_vector_sizes[r];
	}
	assert(total_count == groups.size());
#endif
	idx_t group_count = 0;
	for (hash_t r = 0; r < radix_partitions; r++) {
		group_subset.Slice(groups, sel_vectors[r], sel_vector_sizes[r]);
		payload_subset.Slice(payload, sel_vectors[r], sel_vector_sizes[r]);
		hashes_subset.Slice(hashes, sel_vectors[r], sel_vector_sizes[r]);

		auto &ht_list = radix_partitioned_hts[r];

		if (ht_list.empty() || ht_list.back()->Size() + groups.size() > ht_list.back()->MaxCapacity()) {
			ht_list.push_back(make_unique<GroupedAggregateHashTable>(buffer_manager, group_types, payload_types,
			                                                         bindings, HtEntryType::HT_WIDTH_32));
		}
		group_count += ht_list.back()->AddChunk(group_subset, hashes_subset, payload_subset);
	}
	return group_count;
}

void PartitionableHashTable::Partition() {
	assert(!IsPartitioned());

	vector<GroupedAggregateHashTable *> partition_hts;
	assert(radix_partitioned_hts.size() == 0);
	for (idx_t r = 0; r < radix_partitions; r++) {
		radix_partitioned_hts[r].push_back(make_unique<GroupedAggregateHashTable>(
		    buffer_manager, group_types, payload_types, bindings, HtEntryType::HT_WIDTH_32));
		partition_hts.push_back(radix_partitioned_hts[r].back().get());
	}

	unpartitioned_ht->Partition(partition_hts, radix_mask, RADIX_SHIFT);
	unpartitioned_ht.reset();
	is_partitioned = true;
}

bool PartitionableHashTable::IsPartitioned() {
	return is_partitioned;
}

vector<unique_ptr<GroupedAggregateHashTable>> PartitionableHashTable::GetPartition(idx_t partition) {
	assert(IsPartitioned());
	assert(partition < radix_partitions);
	assert(radix_partitioned_hts.size() > partition);
	return move(radix_partitioned_hts[partition]);
}
unique_ptr<GroupedAggregateHashTable> PartitionableHashTable::GetUnpartitioned() {
	assert(!IsPartitioned());
	return move(unpartitioned_ht);
}

} // namespace duckdb
