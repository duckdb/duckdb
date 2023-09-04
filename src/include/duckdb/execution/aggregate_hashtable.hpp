//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/partitioned_tuple_data.hpp"
#include "duckdb/execution/base_aggregate_hashtable.hpp"
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

struct aggr_ht_entry_t {
public:
	explicit aggr_ht_entry_t(hash_t value_p) : value(value_p) {
	}

	inline bool IsOccupied() const {
		return value != 0;
	}

	inline data_ptr_t GetPointer() const {
		D_ASSERT(IsOccupied());
		return reinterpret_cast<data_ptr_t>(value & POINTER_MASK);
	}
	inline void SetPointer(const data_ptr_t &pointer) {
		// Pointer shouldn't use upper bits
		D_ASSERT((reinterpret_cast<uint64_t>(pointer) & SALT_MASK) == 0);
		// Value should have all 1's in the pointer area
		D_ASSERT((value & POINTER_MASK) == POINTER_MASK);
		// Set upper bits to 1 in pointer so the salt stays intact
		value &= reinterpret_cast<uint64_t>(pointer) | SALT_MASK;
	}

	static inline hash_t ExtractSalt(const hash_t &hash) {
		// Leaves upper bits intact, sets lower bits to all 1's
		return hash | POINTER_MASK;
	}
	inline hash_t GetSalt() const {
		return ExtractSalt(value);
	}
	inline void SetSalt(const hash_t &salt) {
		// Shouldn't be occupied when we set this
		D_ASSERT(!IsOccupied());
		// Salt should have all 1's in the pointer field
		D_ASSERT((salt & POINTER_MASK) == POINTER_MASK);
		// No need to mask, just put the whole thing there
		value = salt;
	}

private:
	//! Upper 16 bits are salt
	static constexpr const hash_t SALT_MASK = 0xFFFF000000000000;
	//! Lower 48 bits are the pointer
	static constexpr const hash_t POINTER_MASK = 0x0000FFFFFFFFFFFF;

	hash_t value;
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
	constexpr static float LOAD_FACTOR = 1.5;

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

	//! Add the given data to the HT, computing the aggregates grouped by the
	//! data in the group chunk. When resize = true, aggregates will not be
	//! computed but instead just assigned.
	idx_t AddChunk(DataChunk &groups, DataChunk &payload, const unsafe_vector<idx_t> &filter);
	idx_t AddChunk(DataChunk &groups, Vector &group_hashes, DataChunk &payload, const unsafe_vector<idx_t> &filter);
	idx_t AddChunk(DataChunk &groups, DataChunk &payload, AggregateType filter);

	//! Fetch the aggregates for specific groups from the HT and place them in the result
	void FetchAggregates(DataChunk &groups, DataChunk &result);

	//! Finds or creates groups in the hashtable using the specified group keys. The addresses vector will be filled
	//! with pointers to the groups in the hash table, and the new_groups selection vector will point to the newly
	//! created groups. The return value is the amount of newly created groups.
	idx_t FindOrCreateGroups(DataChunk &groups, Vector &group_hashes, Vector &addresses_out,
	                         SelectionVector &new_groups_out);
	idx_t FindOrCreateGroups(DataChunk &groups, Vector &addresses_out, SelectionVector &new_groups_out);
	void FindOrCreateGroups(DataChunk &groups, Vector &addresses_out);

	unique_ptr<PartitionedTupleData> &GetPartitionedData();
	shared_ptr<ArenaAllocator> GetAggregateAllocator();

	//! Resize the HT to the specified size. Must be larger than the current size.
	void Resize(idx_t size);
	//! Resets the pointer table of the HT to all 0's
	void ClearPointerTable();
	//! Resets the group count to 0
	void ResetCount();
	//! Set the radix bits for this HT
	void SetRadixBits(idx_t radix_bits);
	//! Initializes the PartitionedTupleData
	void InitializePartitionedData();

	//! Executes the filter(if any) and update the aggregates
	void Combine(GroupedAggregateHashTable &other);
	void Combine(TupleDataCollection &other_data);

	//! Unpins the data blocks
	void UnpinData();

private:
	//! Append state
	struct AggregateHTAppendState {
		AggregateHTAppendState();

		PartitionedTupleDataAppendState append_state;

		Vector ht_offsets;
		Vector hash_salts;
		SelectionVector group_compare_vector;
		SelectionVector no_match_vector;
		SelectionVector empty_vector;
		SelectionVector new_groups;
		Vector addresses;
		unsafe_unique_array<UnifiedVectorFormat> group_data;
		DataChunk group_chunk;
	} state;

	//! The number of radix bits to partition by
	idx_t radix_bits;
	//! The data of the HT
	unique_ptr<PartitionedTupleData> partitioned_data;

	//! Predicates for matching groups (always ExpressionType::COMPARE_EQUAL)
	vector<ExpressionType> predicates;

	//! The number of groups in the HT
	idx_t count;
	//! The capacity of the HT. This can be increased using GroupedAggregateHashTable::Resize
	idx_t capacity;
	//! The hash map (pointer table) of the HT: allocated data and pointer into it
	AllocatedData hash_map;
	aggr_ht_entry_t *entries;
	//! Offset of the hash column in the rows
	idx_t hash_offset;
	//! Bitmask for getting relevant bits from the hashes to determine the position
	hash_t bitmask;

	//! The active arena allocator used by the aggregates for their internal state
	shared_ptr<ArenaAllocator> aggregate_allocator;
	//! Owning arena allocators that this HT has data from
	vector<shared_ptr<ArenaAllocator>> stored_allocators;

private:
	//! Disabled the copy constructor
	GroupedAggregateHashTable(const GroupedAggregateHashTable &) = delete;
	//! Destroy the HT
	void Destroy();

	//! Apply bitmask to get the entry in the HT
	inline idx_t ApplyBitMask(hash_t hash) const;

	//! Does the actual group matching / creation
	idx_t FindOrCreateGroupsInternal(DataChunk &groups, Vector &group_hashes, Vector &addresses,
	                                 SelectionVector &new_groups);

	//! Verify the pointer table of the HT
	void Verify();
};

} // namespace duckdb
