//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/row_operations/row_matcher.hpp"
#include "duckdb/common/types/row/partitioned_tuple_data.hpp"
#include "duckdb/execution/base_aggregate_hashtable.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {

class BlockHandle;
class BufferHandle;

struct FlushMoveState;

//! GroupedAggregateHashTable is a linear probing HT that is used for computing
//! aggregates
/*!
    GroupedAggregateHashTable is a HT that is used for computing aggregates. It takes
   as input the set of groups and the types of the aggregates to compute and
   stores them in the HT. It uses linear probing for collision resolution.
*/
struct AggregateHTScanState {
public:
	AggregateHTScanState() {
	}

	idx_t partition_idx = 0;
	TupleDataScanState scan_states;
};

class GroupedAggregateHashTable : public BaseAggregateHashTable {
public:
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types,
	                          vector<LogicalType> payload_types, const vector<BoundAggregateExpression *> &aggregates,
	                          idx_t initial_capacity = InitialCapacity(), idx_t radix_bits = 0);
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types,
	                          vector<LogicalType> payload_types, vector<AggregateObject> aggregates,
	                          idx_t initial_capacity = InitialCapacity(), idx_t radix_bits = 0);
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types);
	~GroupedAggregateHashTable() override;

public:
	//! The hash table load factor, when a resize is triggered
	constexpr static double LOAD_FACTOR = 1.25;

	//! Get the layout of this HT
	const TupleDataLayout &GetLayout() const;
	//! Number of groups in the HT
	idx_t Count() const;
	//! Initial capacity of the HT
	static idx_t InitialCapacity();
	//! Capacity that can hold 'count' entries without resizing
	static idx_t GetCapacityForCount(idx_t count);
	//! Current capacity of the HT
	idx_t Capacity() const;
	//! Threshold at which to resize the HT
	idx_t ResizeThreshold() const;
	static idx_t ResizeThreshold(idx_t capacity);

	//! Add the given data to the HT, computing the aggregates grouped by the
	//! data in the group chunk. When resize = true, aggregates will not be
	//! computed but instead just assigned.
	idx_t AddChunk(DataChunk &groups, DataChunk &payload, const unsafe_vector<idx_t> &filter);
	idx_t AddChunk(DataChunk &groups, Vector &group_hashes, DataChunk &payload, const unsafe_vector<idx_t> &filter);
	idx_t AddChunk(DataChunk &groups, DataChunk &payload, AggregateType filter);
	optional_idx TryAddCompressedGroups(DataChunk &groups, DataChunk &payload, const unsafe_vector<idx_t> &filter);
	optional_idx TryAddDictionaryGroups(DataChunk &groups, DataChunk &payload, const unsafe_vector<idx_t> &filter);
	optional_idx TryAddConstantGroups(DataChunk &groups, DataChunk &payload, const unsafe_vector<idx_t> &filter);

	//! Fetch the aggregates for specific groups from the HT and place them in the result
	void FetchAggregates(DataChunk &groups, DataChunk &result);

	void InitializeScan(AggregateHTScanState &scan_state);
	bool Scan(AggregateHTScanState &scan_state, DataChunk &distinct_rows, DataChunk &payload_rows);

	//! Finds or creates groups in the hashtable using the specified group keys. The addresses vector will be filled
	//! with pointers to the groups in the hash table, and the new_groups selection vector will point to the newly
	//! created groups. The return value is the amount of newly created groups.
	idx_t FindOrCreateGroups(DataChunk &groups, Vector &group_hashes, Vector &addresses_out,
	                         SelectionVector &new_groups_out);
	idx_t FindOrCreateGroups(DataChunk &groups, Vector &addresses_out, SelectionVector &new_groups_out);
	void FindOrCreateGroups(DataChunk &groups, Vector &addresses_out);

	const PartitionedTupleData &GetPartitionedData() const;
	unique_ptr<PartitionedTupleData> AcquirePartitionedData();
	void Abandon();
	void Repartition();
	shared_ptr<ArenaAllocator> GetAggregateAllocator();

	//! Resize the HT to the specified size. Must be larger than the current size.
	void Resize(idx_t size);
	//! Resets the pointer table of the HT to all 0's
	void ClearPointerTable();
	//! Set the radix bits for this HT
	void SetRadixBits(idx_t radix_bits);
	//! Get the radix bits for this HT
	idx_t GetRadixBits() const;
	//! Get the total amount of data sunk into this HT
	idx_t GetSinkCount() const;
	//! Skips lookups from here on out
	void SkipLookups();

	//! Executes the filter(if any) and update the aggregates
	void Combine(GroupedAggregateHashTable &other);
	void Combine(TupleDataCollection &other_data, optional_ptr<atomic<double>> progress = nullptr);

private:
	ClientContext &context;
	//! Efficiently matches groups
	RowMatcher row_matcher;

	struct AggregateDictionaryState {
		AggregateDictionaryState();

		//! The current dictionary vector id (if any)
		string dictionary_id;
		DataChunk unique_values;
		Vector hashes;
		Vector new_dictionary_pointers;
		SelectionVector unique_entries;
		unique_ptr<Vector> dictionary_addresses;
		unsafe_unique_array<bool> found_entry;
		idx_t capacity = 0;
	};

	//! Append state
	struct AggregateHTAppendState {
		AggregateHTAppendState();

		PartitionedTupleDataAppendState partitioned_append_state;
		PartitionedTupleDataAppendState unpartitioned_append_state;

		Vector ht_offsets;
		Vector hash_salts;
		SelectionVector group_compare_vector;
		SelectionVector no_match_vector;
		SelectionVector empty_vector;
		SelectionVector new_groups;
		Vector addresses;
		unsafe_unique_array<UnifiedVectorFormat> group_data;
		DataChunk group_chunk;
		AggregateDictionaryState dict_state;
	} state;

	//! If we have this many or more radix bits, we use the unpartitioned data collection too
	static constexpr idx_t UNPARTITIONED_RADIX_BITS_THRESHOLD = 3;
	//! The number of radix bits to partition by
	idx_t radix_bits;
	//! The data of the HT
	unique_ptr<PartitionedTupleData> partitioned_data;
	unique_ptr<PartitionedTupleData> unpartitioned_data;

	//! Predicates for matching groups (always ExpressionType::COMPARE_EQUAL)
	vector<ExpressionType> predicates;

	//! The number of groups in the HT
	idx_t count;
	//! The capacity of the HT. This can be increased using GroupedAggregateHashTable::Resize
	idx_t capacity;
	//! The hash map (pointer table) of the HT: allocated data and pointer into it
	AllocatedData hash_map;
	ht_entry_t *entries;
	//! Offset of the hash column in the rows
	idx_t hash_offset;
	//! Bitmask for getting relevant bits from the hashes to determine the position
	hash_t bitmask;

	//! How many tuples went into this HT (before de-duplication)
	idx_t sink_count;
	//! If true, we just append, skipping HT lookups
	bool skip_lookups;

	//! The active arena allocator used by the aggregates for their internal state
	shared_ptr<ArenaAllocator> aggregate_allocator;
	//! Owning arena allocators that this HT has data from
	vector<shared_ptr<ArenaAllocator>> stored_allocators;

private:
	//! Disabled the copy constructor
	GroupedAggregateHashTable(const GroupedAggregateHashTable &) = delete;
	//! Destroy the HT
	void Destroy();

	//! Initializes the PartitionedTupleData
	void InitializePartitionedData();
	//! Initializes the PartitionedTupleData that only has 1 partition
	void InitializeUnpartitionedData();
	//! Apply bitmask to get the entry in the HT
	inline idx_t ApplyBitMask(hash_t hash) const;
	//! Reinserts tuples (triggered by Resize)
	void ReinsertTuples(PartitionedTupleData &data);

	void UpdateAggregates(DataChunk &payload, const unsafe_vector<idx_t> &filter);

	//! Does the actual group matching / creation
	idx_t FindOrCreateGroupsInternal(DataChunk &groups, Vector &group_hashes, Vector &addresses,
	                                 SelectionVector &new_groups);

	//! Verify the pointer table of the HT
	void Verify();
};

} // namespace duckdb
