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
// [SALT][PAGE_NR][PAGE_OFFSET]
// [SALT] are the high bits of the hash value, e.g. 16 for 64 bit hashes
// [PAGE_NR] is the buffer managed payload page index
// [PAGE_OFFSET] is the logical entry offset into said payload page

// NOTE: PAGE_NR and PAGE_OFFSET are reversed for 64 bit HTs because struct packing

// payload layout
// [VALIDITY][GROUPS][HASH][PADDING][PAYLOAD]
// [VALIDITY] is the validity bits of the data columns (including the HASH)
// [GROUPS] is the group data, could be multiple values, fixed size, strings are elsewhere
// [HASH] is the hash data of the groups
// [PADDING] is gunk data to align payload properly
// [PAYLOAD] is the payload (i.e. the aggregate states)
struct aggr_ht_entry_64 {
	uint16_t salt;
	uint16_t page_offset;
	uint32_t page_nr; // this has to come last because alignment
};

struct aggr_ht_entry_32 {
	uint8_t salt;
	uint8_t page_nr;
	uint16_t page_offset;
};

enum HtEntryType { HT_WIDTH_32, HT_WIDTH_64 };

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
	unique_ptr<UnifiedVectorFormat[]> group_data;
	DataChunk group_chunk;
};

class GroupedAggregateHashTable : public BaseAggregateHashTable {
public:
	//! The hash table load factor, when a resize is triggered
	constexpr static float LOAD_FACTOR = 1.5;
	constexpr static uint8_t HASH_WIDTH = sizeof(hash_t);

public:
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types,
	                          vector<LogicalType> payload_types, const vector<BoundAggregateExpression *> &aggregates,
	                          HtEntryType entry_type = HtEntryType::HT_WIDTH_64,
	                          idx_t initial_capacity = InitialCapacity());
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types,
	                          vector<LogicalType> payload_types, vector<AggregateObject> aggregates,
	                          HtEntryType entry_type = HtEntryType::HT_WIDTH_64,
	                          idx_t initial_capacity = InitialCapacity());
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types);
	~GroupedAggregateHashTable() override;

public:
	//! Add the given data to the HT, computing the aggregates grouped by the
	//! data in the group chunk. When resize = true, aggregates will not be
	//! computed but instead just assigned.
	idx_t AddChunk(AggregateHTAppendState &state, DataChunk &groups, DataChunk &payload, const vector<idx_t> &filter);
	idx_t AddChunk(AggregateHTAppendState &state, DataChunk &groups, Vector &group_hashes, DataChunk &payload,
	               const vector<idx_t> &filter);
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

	TupleDataCollection &GetDataCollection() {
		return *data_collection;
	}

	idx_t Count() {
		return data_collection->Count();
	}

	static idx_t InitialCapacity();
	idx_t Capacity() {
		return capacity;
	}

	idx_t ResizeThreshold();
	idx_t MaxCapacity();
	static idx_t GetMaxCapacity(HtEntryType entry_type, idx_t tuple_size);

	void Partition(vector<GroupedAggregateHashTable *> &partition_hts, idx_t radix_bits);
	void InitializeFirstPart();

	void Finalize();

private:
	HtEntryType entry_type;

	//! The capacity of the HT. This can be increased using GroupedAggregateHashTable::Resize
	idx_t capacity;
	//! Tuple width
	idx_t tuple_size;
	//! Tuples per block
	idx_t tuples_per_block;
	//! The data of the HT
	unique_ptr<TupleDataCollection> data_collection;
	TupleDataAppendState td_append_state;
	vector<data_ptr_t> payload_hds_ptrs;

	//! The hashes of the HT
	BufferHandle hashes_hdl;
	data_ptr_t hashes_hdl_ptr;
	idx_t hash_offset; // Offset into the layout of the hash column

	hash_t hash_prefix_shift;

	//! Bitmask for getting relevant bits from the hashes to determine the position
	hash_t bitmask;

	bool is_finalized;

	vector<ExpressionType> predicates;

private:
	GroupedAggregateHashTable(const GroupedAggregateHashTable &) = delete;

	void Destroy();
	void Verify();
	template <class ENTRY>
	void VerifyInternal();
	//! Resize the HT to the specified size. Must be larger than the current size.
	template <class ENTRY>
	void Resize(idx_t size);
	//! Initializes the first part of the HT
	template <class ENTRY>
	void InitializeHashes();
	//! Does the actual group matching / creation
	template <class ENTRY>
	idx_t FindOrCreateGroupsInternal(DataChunk &groups, Vector &group_hashes_v, Vector &addresses_v,
	                                 SelectionVector &new_groups);
	//! Updates payload_hds_ptrs with the new pointers (after appending to data_collection)
	void UpdateBlockPointers();
	template <class ENTRY>
	idx_t FindOrCreateGroupsInternal(AggregateHTAppendState &state, DataChunk &groups, Vector &group_hashes,
	                                 Vector &addresses, SelectionVector &new_groups);
};

} // namespace duckdb
