//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_collection.hpp"
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

// two part hash table
// hashes and payload
// hashes layout:
// [48 BIT POINTER][16 BIT SALT]
//
// payload layout
// [VALIDITY][GROUPS][HASH][PADDING][PAYLOAD]
// [VALIDITY] is the validity bits of the data columns (including the HASH)
// [GROUPS] is the group data, could be multiple values, fixed size, strings are elsewhere
// [HASH] is the hash data of the groups
// [PADDING] is gunk data to align payload properly
// [PAYLOAD] is the payload (i.e. the aggregate states)

struct aggr_ht_entry_t {
public:
	inline bool IsOccupied() const {
		return value.hash.hash != 0;
	}
	inline void SetOccupied() {
		value.entry.pointer = 1;
	}

	inline data_ptr_t GetPointer() const {
		return reinterpret_cast<data_ptr_t>(value.entry.pointer);
	}
	inline void SetPointer(data_ptr_t pointer_p) {
		value.entry.pointer = reinterpret_cast<uint64_t>(pointer_p);
	}

	constexpr static auto HASH_WIDTH = sizeof(hash_t);
	static constexpr const auto HASH_PREFIX_SHIFT = (HASH_WIDTH - sizeof(uint16_t)) * 8;
	inline void SetSalt(hash_t hash) {
		value.entry.salt = ExtractSalt(hash);
	}
	inline void SetSaltOverwrite(hash_t hash) {
		value.hash.hash = hash;
	}
	inline uint16_t GetSalt() const {
		return value.entry.salt;
	}
	static inline uint16_t ExtractSalt(hash_t hash) {
		return hash >> HASH_PREFIX_SHIFT;
	}

private:
	union {
		struct {
			uint64_t pointer : 48;
			uint16_t salt;
		} entry;
		struct {
			hash_t hash;
		} hash;
	} value;
};

struct AggregateHTScanState {
	mutex lock;
	TupleDataScanState scan_state;
};

struct AggregateHTAppendState {
	AggregateHTAppendState();

	Vector ht_offsets;
	Vector hash_salts;
	SelectionVector group_compare_vector;
	SelectionVector no_match_vector;
	SelectionVector empty_vector;
	SelectionVector new_groups;
	Vector addresses;
	unsafe_unique_array<UnifiedVectorFormat> group_data;
	DataChunk group_chunk;

	TupleDataChunkState chunk_state;
	bool chunk_state_initialized;
};

class GroupedAggregateHashTable : public BaseAggregateHashTable {
public:
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types,
	                          vector<LogicalType> payload_types, const vector<BoundAggregateExpression *> &aggregates,
	                          idx_t initial_capacity = InitialCapacity());
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types,
	                          vector<LogicalType> payload_types, vector<AggregateObject> aggregates,
	                          idx_t initial_capacity = InitialCapacity());
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types);
	~GroupedAggregateHashTable() override;

public:
	idx_t Count() const;
	static idx_t InitialCapacity();
	static idx_t SinkCapacity();
	idx_t Capacity() const;
	idx_t ResizeThreshold() const;

	TupleDataCollection &GetDataCollection();
	idx_t DataSize() const;
	static idx_t FirstPartSize(idx_t count);
	idx_t TotalSize() const;

	//! Add the given data to the HT, computing the aggregates grouped by the
	//! data in the group chunk. When resize = true, aggregates will not be
	//! computed but instead just assigned.
	idx_t AddChunk(AggregateHTAppendState &state, DataChunk &groups, DataChunk &payload,
	               const unsafe_vector<idx_t> &filter);
	idx_t AddChunk(AggregateHTAppendState &state, DataChunk &groups, Vector &group_hashes, DataChunk &payload,
	               const unsafe_vector<idx_t> &filter);
	idx_t AddChunk(AggregateHTAppendState &state, DataChunk &groups, DataChunk &payload, AggregateType filter);

	//! Scan the HT starting from the scan_position until the result and group
	//! chunks are filled. scan_position will be updated by this function.
	//! Returns the amount of elements found.
	idx_t Scan(TupleDataParallelScanState &gstate, TupleDataLocalScanState &lstate, DataChunk &result);

	//! Fetch the aggregates for specific groups from the HT and place them in the result
	void FetchAggregates(DataChunk &groups, DataChunk &result);

	//! Finds or creates groups in the hashtable using the specified group keys. The addresses vector will be filled
	//! with pointers to the groups in the hash table, and the new_groups selection vector will point to the newly
	//! created groups. The return value is the amount of newly created groups.
	idx_t FindOrCreateGroups(AggregateHTAppendState &state, DataChunk &groups, Vector &group_hashes,
	                         Vector &addresses_out, SelectionVector &new_groups_out);
	idx_t FindOrCreateGroups(AggregateHTAppendState &state, DataChunk &groups, Vector &addresses_out,
	                         SelectionVector &new_groups_out);
	void FindOrCreateGroups(AggregateHTAppendState &state, DataChunk &groups, Vector &addresses_out);

	//! Executes the filter(if any) and update the aggregates
	void Combine(GroupedAggregateHashTable &other);

	//! Appends the data in the other HT to this one
	void Append(GroupedAggregateHashTable &other);

	void Partition(vector<GroupedAggregateHashTable *> &partition_hts, idx_t radix_bits, bool sink_done);
	void InitializeFirstPart();

	void Finalize();

private:
	//! The hash table load factor, when a resize is triggered
	constexpr static float LOAD_FACTOR = 1.5;
	//! The size of the first part of the HT, should fit in L2 cache
	constexpr static idx_t FIRST_PART_SINK_SIZE = 1048576;

	//! The capacity of the HT. This can be increased using GroupedAggregateHashTable::Resize
	idx_t capacity;
	//! Tuple width
	idx_t tuple_size;
	//! Tuples per block
	idx_t tuples_per_block;
	//! The data of the HT
	unique_ptr<TupleDataCollection> data_collection;
	TupleDataPinState td_pin_state;

	//! The hashes of the HT
	AllocatedData hash_map;
	aggr_ht_entry_t *entries;
	idx_t hash_offset; // Offset into the layout of the hash column

	hash_t hash_prefix_shift;

	//! Bitmask for getting relevant bits from the hashes to determine the position
	hash_t bitmask;

	bool is_finalized;

	vector<ExpressionType> predicates;

	//! The active arena allocator used by the aggregates for their internal state
	shared_ptr<ArenaAllocator> aggregate_allocator;
	//! Owning arena allocators that this HT has data from
	vector<shared_ptr<ArenaAllocator>> stored_allocators;

private:
	GroupedAggregateHashTable(const GroupedAggregateHashTable &) = delete;

	void Destroy();
	void Verify();
	//! Resize the HT to the specified size. Must be larger than the current size.
	void Resize(idx_t size);
	//! Does the actual group matching / creation
	idx_t FindOrCreateGroupsInternal(DataChunk &groups, Vector &group_hashes_v, Vector &addresses_v,
	                                 SelectionVector &new_groups);
	idx_t FindOrCreateGroupsInternal(AggregateHTAppendState &state, DataChunk &groups, Vector &group_hashes,
	                                 Vector &addresses, SelectionVector &new_groups);
};

} // namespace duckdb
