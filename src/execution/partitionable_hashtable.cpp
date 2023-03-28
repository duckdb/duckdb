#include "duckdb/execution/partitionable_hashtable.hpp"

namespace duckdb {

RadixPartitionInfo::RadixPartitionInfo(const idx_t n_partitions_upper_bound)
    : n_partitions(PreviousPowerOfTwo(n_partitions_upper_bound)),
      radix_bits(RadixPartitioning::RadixBits(n_partitions)), radix_mask(RadixPartitioning::Mask(radix_bits)),
      radix_shift(RadixPartitioning::Shift(radix_bits)) {

	D_ASSERT(n_partitions > 0);
	D_ASSERT(n_partitions <= 256);
	D_ASSERT(IsPowerOfTwo(n_partitions));
	D_ASSERT(radix_bits <= 8);
}

PartitionableHashTable::PartitionableHashTable(ClientContext &context, Allocator &allocator,
                                               RadixPartitionInfo &partition_info_p, vector<LogicalType> group_types_p,
                                               vector<LogicalType> payload_types_p,
                                               vector<BoundAggregateExpression *> bindings_p)
    : context(context), allocator(allocator), group_types(std::move(group_types_p)),
      payload_types(std::move(payload_types_p)), bindings(std::move(bindings_p)), is_partitioned(false),
      partition_info(partition_info_p), hashes(LogicalType::HASH), hashes_subset(LogicalType::HASH) {

	sel_vectors.resize(partition_info.n_partitions);
	sel_vector_sizes.resize(partition_info.n_partitions);
	group_subset.Initialize(allocator, group_types);
	if (!payload_types.empty()) {
		payload_subset.Initialize(allocator, payload_types);
	}

	for (hash_t r = 0; r < partition_info.n_partitions; r++) {
		sel_vectors[r].Initialize();
	}

	RowLayout layout;
	layout.Initialize(group_types, AggregateObject::CreateAggregateObjects(bindings));
	tuple_size = layout.GetRowWidth();
}

HtEntryType PartitionableHashTable::GetHTEntrySize() {
	// we need at least STANDARD_VECTOR_SIZE entries to fit in the hash table
	if (GroupedAggregateHashTable::GetMaxCapacity(HtEntryType::HT_WIDTH_32, tuple_size) < STANDARD_VECTOR_SIZE) {
		return HtEntryType::HT_WIDTH_64;
	}
	return HtEntryType::HT_WIDTH_32;
}

idx_t PartitionableHashTable::ListAddChunk(HashTableList &list, DataChunk &groups, Vector &group_hashes,
                                           DataChunk &payload, const vector<idx_t> &filter) {
	// If this is false, a single AddChunk would overflow the max capacity
	D_ASSERT(list.empty() || groups.size() <= list.back()->MaxCapacity());
	if (list.empty() || list.back()->Count() + groups.size() >= list.back()->MaxCapacity()) {
		idx_t new_capacity = GroupedAggregateHashTable::InitialCapacity();
		if (!list.empty()) {
			new_capacity = list.back()->Capacity();
			// early release first part of ht and prevent adding of more data
			list.back()->Finalize();
		}
		list.push_back(make_unique<GroupedAggregateHashTable>(context, allocator, group_types, payload_types, bindings,
		                                                      GetHTEntrySize(), new_capacity));
	}
	return list.back()->AddChunk(append_state, groups, group_hashes, payload, filter);
}

idx_t PartitionableHashTable::AddChunk(DataChunk &groups, DataChunk &payload, bool do_partition,
                                       const vector<idx_t> &filter) {
	groups.Hash(hashes);

	// we partition when we are asked to or when the unpartitioned ht runs out of space
	if (!IsPartitioned() && do_partition) {
		Partition();
	}

	if (!IsPartitioned()) {
		return ListAddChunk(unpartitioned_hts, groups, hashes, payload, filter);
	}

	// makes no sense to do this with 1 partition
	D_ASSERT(partition_info.n_partitions > 0);

	for (hash_t r = 0; r < partition_info.n_partitions; r++) {
		sel_vector_sizes[r] = 0;
	}

	hashes.Flatten(groups.size());
	auto hashes_ptr = FlatVector::GetData<hash_t>(hashes);

	// Determine for every partition how much data will be sinked into it
	for (idx_t i = 0; i < groups.size(); i++) {
		auto partition = partition_info.GetHashPartition(hashes_ptr[i]);
		D_ASSERT(partition < partition_info.n_partitions);
		sel_vectors[partition].set_index(sel_vector_sizes[partition]++, i);
	}

#ifdef DEBUG
	// make sure we have lost no rows
	idx_t total_count = 0;
	for (idx_t r = 0; r < partition_info.n_partitions; r++) {
		total_count += sel_vector_sizes[r];
	}
	D_ASSERT(total_count == groups.size());
#endif
	idx_t group_count = 0;
	for (hash_t r = 0; r < partition_info.n_partitions; r++) {
		group_subset.Slice(groups, sel_vectors[r], sel_vector_sizes[r]);
		if (!payload_types.empty()) {
			payload_subset.Slice(payload, sel_vectors[r], sel_vector_sizes[r]);
		} else {
			payload_subset.SetCardinality(sel_vector_sizes[r]);
		}
		hashes_subset.Slice(hashes, sel_vectors[r], sel_vector_sizes[r]);

		group_count += ListAddChunk(radix_partitioned_hts[r], group_subset, hashes_subset, payload_subset, filter);
	}
	return group_count;
}

void PartitionableHashTable::Partition() {
	D_ASSERT(!IsPartitioned());
	D_ASSERT(radix_partitioned_hts.empty());
	D_ASSERT(partition_info.n_partitions > 1);

	vector<GroupedAggregateHashTable *> partition_hts(partition_info.n_partitions);
	radix_partitioned_hts.resize(partition_info.n_partitions);
	for (auto &unpartitioned_ht : unpartitioned_hts) {
		for (idx_t r = 0; r < partition_info.n_partitions; r++) {
			radix_partitioned_hts[r].push_back(make_unique<GroupedAggregateHashTable>(
			    context, allocator, group_types, payload_types, bindings, GetHTEntrySize()));
			partition_hts[r] = radix_partitioned_hts[r].back().get();
		}
		unpartitioned_ht->Partition(partition_hts, partition_info.radix_bits);
		unpartitioned_ht.reset();
	}
	unpartitioned_hts.clear();
	is_partitioned = true;
}

bool PartitionableHashTable::IsPartitioned() {
	return is_partitioned;
}

HashTableList PartitionableHashTable::GetPartition(idx_t partition) {
	D_ASSERT(IsPartitioned());
	D_ASSERT(partition < partition_info.n_partitions);
	D_ASSERT(radix_partitioned_hts.size() > partition);
	return std::move(radix_partitioned_hts[partition]);
}
HashTableList PartitionableHashTable::GetUnpartitioned() {
	D_ASSERT(!IsPartitioned());
	return std::move(unpartitioned_hts);
}

void PartitionableHashTable::Finalize() {
	if (IsPartitioned()) {
		for (auto &ht_list : radix_partitioned_hts) {
			for (auto &ht : ht_list) {
				D_ASSERT(ht);
				ht->Finalize();
			}
		}
	} else {
		for (auto &ht : unpartitioned_hts) {
			D_ASSERT(ht);
			ht->Finalize();
		}
	}
}

} // namespace duckdb
