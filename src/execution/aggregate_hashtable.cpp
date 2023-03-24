#include "duckdb/execution/aggregate_hashtable.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cmath>

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types, vector<LogicalType> payload_types,
                                                     const vector<BoundAggregateExpression *> &bindings,
                                                     HtEntryType entry_type)
    : GroupedAggregateHashTable(context, allocator, std::move(group_types), std::move(payload_types),
                                AggregateObject::CreateAggregateObjects(bindings), entry_type) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types)
    : GroupedAggregateHashTable(context, allocator, std::move(group_types), {}, vector<AggregateObject>()) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types_p,
                                                     vector<LogicalType> payload_types_p,
                                                     vector<AggregateObject> aggregate_objects_p,
                                                     HtEntryType entry_type)
    : BaseAggregateHashTable(context, allocator, aggregate_objects_p, std::move(payload_types_p)),
      entry_type(entry_type), capacity(0), is_finalized(false), ht_offsets(LogicalTypeId::BIGINT),
      hash_salts(LogicalTypeId::SMALLINT), group_compare_vector(STANDARD_VECTOR_SIZE),
      no_match_vector(STANDARD_VECTOR_SIZE), empty_vector(STANDARD_VECTOR_SIZE) {

	// Append hash column to the end and initialise the row layout
	group_types_p.emplace_back(LogicalType::HASH);
	layout.Initialize(std::move(group_types_p), std::move(aggregate_objects_p));
	tuple_size = layout.GetRowWidth();
	tuples_per_block = Storage::BLOCK_SIZE / tuple_size;

	// HT layout
	hash_offset = layout.GetOffsets()[layout.ColumnCount() - 1];
	data_collection = make_unique<TupleDataCollection>(buffer_manager, layout);
	data_collection->InitializeAppend(append_state, TupleDataPinProperties::KEEP_EVERYTHING_PINNED);

	hashes_hdl = buffer_manager.Allocate(Storage::BLOCK_SIZE);
	hashes_hdl_ptr = hashes_hdl.Ptr();

	switch (entry_type) {
	case HtEntryType::HT_WIDTH_64: {
		hash_prefix_shift = (HASH_WIDTH - sizeof(aggr_ht_entry_64::salt)) * 8;
		Resize<aggr_ht_entry_64>(STANDARD_VECTOR_SIZE * 2L);
		break;
	}
	case HtEntryType::HT_WIDTH_32: {
		hash_prefix_shift = (HASH_WIDTH - sizeof(aggr_ht_entry_32::salt)) * 8;
		Resize<aggr_ht_entry_32>(STANDARD_VECTOR_SIZE * 2L);
		break;
	}
	default:
		throw InternalException("Unknown HT entry width");
	}

	predicates.resize(layout.ColumnCount() - 1, ExpressionType::COMPARE_EQUAL);
}

GroupedAggregateHashTable::~GroupedAggregateHashTable() {
	Destroy();
}

template <class FUNC>
void GroupedAggregateHashTable::PayloadApply(FUNC fun) {
	if (Count() == 0) {
		return;
	}
	idx_t apply_entries = Count();
	idx_t page_nr = 0;
	idx_t page_offset = 0;

	for (auto &payload_chunk_ptr : payload_hds_ptrs) {
		auto this_entries = MinValue(tuples_per_block, apply_entries);
		page_offset = 0;
		for (data_ptr_t ptr = payload_chunk_ptr, end = payload_chunk_ptr + this_entries * tuple_size; ptr < end;
		     ptr += tuple_size) {
			fun(page_nr, page_offset++, ptr);
		}
		apply_entries -= this_entries;
		page_nr++;
	}
	D_ASSERT(apply_entries == 0);
}

void GroupedAggregateHashTable::Destroy() {
	// check if there is a destructor
	bool has_destructor = false;
	for (auto &aggr : layout.GetAggregates()) {
		if (aggr.function.destructor) {
			has_destructor = true;
		}
	}
	if (!has_destructor) {
		return;
	}
	// there are aggregates with destructors: loop over the hash table
	// and call the destructor method for each of the aggregates
	data_ptr_t data_pointers[STANDARD_VECTOR_SIZE];
	Vector state_vector(LogicalType::POINTER, (data_ptr_t)data_pointers);
	idx_t count = 0;

	PayloadApply([&](idx_t page_nr, idx_t page_offset, data_ptr_t ptr) {
		data_pointers[count++] = ptr;
		if (count == STANDARD_VECTOR_SIZE) {
			RowOperations::DestroyStates(layout, state_vector, count);
			count = 0;
		}
	});
	RowOperations::DestroyStates(layout, state_vector, count);
}

template <class ENTRY>
void GroupedAggregateHashTable::VerifyInternal() {
	auto hashes_ptr = (ENTRY *)hashes_hdl_ptr;
	idx_t count = 0;
	for (idx_t i = 0; i < capacity; i++) {
		if (hashes_ptr[i].page_nr > 0) {
			D_ASSERT(hashes_ptr[i].page_offset < tuples_per_block);
			D_ASSERT(hashes_ptr[i].page_nr <= payload_hds_ptrs.size());
			auto ptr = payload_hds_ptrs[hashes_ptr[i].page_nr - 1] + ((hashes_ptr[i].page_offset) * tuple_size);
			auto hash = Load<hash_t>(ptr + hash_offset);
			D_ASSERT((hashes_ptr[i].salt) == (hash >> hash_prefix_shift));

			count++;
		}
	}
	(void)count;
	D_ASSERT(count == Count());
}

idx_t GroupedAggregateHashTable::GetMaxCapacity(HtEntryType entry_type, idx_t tuple_size) {
	idx_t max_pages;
	idx_t max_tuples;

	switch (entry_type) {
	case HtEntryType::HT_WIDTH_32:
		max_pages = NumericLimits<uint8_t>::Maximum();
		max_tuples = NumericLimits<uint16_t>::Maximum();
		break;
	case HtEntryType::HT_WIDTH_64:
		max_pages = NumericLimits<uint32_t>::Maximum();
		max_tuples = NumericLimits<uint16_t>::Maximum();
		break;
	default:
		throw InternalException("Unsupported hash table width");
	}

	return max_pages * MinValue(max_tuples, (idx_t)Storage::BLOCK_SIZE / tuple_size);
}

idx_t GroupedAggregateHashTable::MaxCapacity() {
	return GetMaxCapacity(entry_type, tuple_size);
}

void GroupedAggregateHashTable::Verify() {
#ifdef DEBUG
	switch (entry_type) {
	case HtEntryType::HT_WIDTH_32:
		VerifyInternal<aggr_ht_entry_32>();
		break;
	case HtEntryType::HT_WIDTH_64:
		VerifyInternal<aggr_ht_entry_64>();
		break;
	}
#endif
}

template <class ENTRY>
void GroupedAggregateHashTable::Resize(idx_t size) {
	Verify();

	D_ASSERT(!is_finalized);

	if (size <= capacity) {
		throw InternalException("Cannot downsize a hash table!");
	}
	D_ASSERT(size >= STANDARD_VECTOR_SIZE);

	// size needs to be a power of 2
	D_ASSERT((size & (size - 1)) == 0);
	bitmask = size - 1;

	auto byte_size = size * sizeof(ENTRY);
	if (byte_size > (idx_t)Storage::BLOCK_SIZE) {
		hashes_hdl = buffer_manager.Allocate(byte_size);
		hashes_hdl_ptr = hashes_hdl.Ptr();
	}
	memset(hashes_hdl_ptr, 0, byte_size);
	hashes_end_ptr = hashes_hdl_ptr + byte_size;
	capacity = size;

	auto hashes_arr = (ENTRY *)hashes_hdl_ptr;

	PayloadApply([&](idx_t page_nr, idx_t page_offset, data_ptr_t ptr) {
		auto hash = Load<hash_t>(ptr + hash_offset);
		D_ASSERT((hash & bitmask) == (hash % capacity));
		auto entry_idx = (idx_t)hash & bitmask;
		while (hashes_arr[entry_idx].page_nr > 0) {
			entry_idx++;
			if (entry_idx >= capacity) {
				entry_idx = 0;
			}
		}

		D_ASSERT(!hashes_arr[entry_idx].page_nr);
		D_ASSERT(hash >> hash_prefix_shift <= NumericLimits<uint16_t>::Maximum());

		hashes_arr[entry_idx].salt = hash >> hash_prefix_shift;
		hashes_arr[entry_idx].page_nr = page_nr + 1;
		hashes_arr[entry_idx].page_offset = page_offset;
	});

	Verify();
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload, AggregateType filter) {
	vector<idx_t> aggregate_filter;

	auto &aggregates = layout.GetAggregates();
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		if (aggregate.aggr_type == filter) {
			aggregate_filter.push_back(i);
		}
	}
	return AddChunk(groups, payload, aggregate_filter);
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload, const vector<idx_t> &filter) {
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);

	return AddChunk(groups, hashes, payload, filter);
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, Vector &group_hashes, DataChunk &payload,
                                          const vector<idx_t> &filter) {
	D_ASSERT(!is_finalized);
	if (groups.size() == 0) {
		return 0;
	}

#ifdef DEBUG
	D_ASSERT(groups.ColumnCount() + 1 == layout.ColumnCount());
	for (idx_t i = 0; i < groups.ColumnCount(); i++) {
		D_ASSERT(groups.GetTypes()[i] == layout.GetTypes()[i]);
	}
#endif

	Vector addresses(LogicalType::POINTER);
	SelectionVector new_groups(STANDARD_VECTOR_SIZE); // Dummy
	auto new_group_count = FindOrCreateGroups(groups, group_hashes, addresses, new_groups);
	VectorOperations::AddInPlace(addresses, layout.GetAggrOffset(), payload.size());

	// Now every cell has an entry, update the aggregates
	auto &aggregates = layout.GetAggregates();
	idx_t filter_idx = 0;
	idx_t payload_idx = 0;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i];
		if (filter_idx >= filter.size() || i < filter[filter_idx]) {
			// Skip all the aggregates that are not in the filter
			payload_idx += aggr.child_count;
			VectorOperations::AddInPlace(addresses, aggr.payload_size, payload.size());
			continue;
		}
		D_ASSERT(i == filter[filter_idx]);

		if (aggr.aggr_type != AggregateType::DISTINCT && aggr.filter) {
			RowOperations::UpdateFilteredStates(filter_set.GetFilterData(i), aggr, addresses, payload, payload_idx);
		} else {
			RowOperations::UpdateStates(aggr, addresses, payload, payload_idx, payload.size());
		}

		// Move to the next aggregate
		payload_idx += aggr.child_count;
		VectorOperations::AddInPlace(addresses, aggr.payload_size, payload.size());
		filter_idx++;
	}

	Verify();
	return new_group_count;
}

void GroupedAggregateHashTable::FetchAggregates(DataChunk &groups, DataChunk &result) {
	groups.Verify();
	D_ASSERT(groups.ColumnCount() + 1 == layout.ColumnCount());
	for (idx_t i = 0; i < result.ColumnCount(); i++) {
		D_ASSERT(result.data[i].GetType() == payload_types[i]);
	}
	result.SetCardinality(groups);
	if (groups.size() == 0) {
		return;
	}
	// find the groups associated with the addresses
	// FIXME: this should not use the FindOrCreateGroups, creating them is unnecessary
	Vector addresses(LogicalType::POINTER);
	FindOrCreateGroups(groups, addresses);
	// now fetch the aggregates
	RowOperations::FinalizeStates(layout, addresses, result, 0);
}

template <class ENTRY>
idx_t GroupedAggregateHashTable::FindOrCreateGroupsInternal(DataChunk &groups, Vector &group_hashes_v,
                                                            Vector &addresses_v, SelectionVector &new_groups_out) {
	D_ASSERT(!is_finalized);
	D_ASSERT(capacity - Count() >= groups.size());
	D_ASSERT(groups.ColumnCount() + 1 == layout.ColumnCount());
	D_ASSERT(capacity - Count() >= groups.size()); // we need to be able to fit at least one vector of data
	D_ASSERT(group_hashes_v.GetType() == LogicalType::HASH);
	D_ASSERT(ht_offsets.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(ht_offsets.GetType() == LogicalType::BIGINT);
	D_ASSERT(addresses_v.GetType() == LogicalType::POINTER);
	D_ASSERT(hash_salts.GetType() == LogicalType::SMALLINT);

	if (Count() + groups.size() > MaxCapacity()) {
		throw InternalException("Hash table capacity reached");
	}

	// Resize at 50% capacity, also need to fit the entire vector
	if (capacity - Count() <= groups.size() || Count() > capacity / LOAD_FACTOR) {
		Resize<ENTRY>(capacity * 2);
	}

	group_hashes_v.Flatten(groups.size());
	auto group_hashes = FlatVector::GetData<hash_t>(group_hashes_v);

	addresses_v.Flatten(groups.size());
	auto addresses = FlatVector::GetData<data_ptr_t>(addresses_v);

	// Compute the entry in the table based on the hash using a modulo
	UnaryExecutor::Execute<hash_t, uint64_t>(group_hashes_v, ht_offsets, groups.size(), [&](hash_t element) {
		D_ASSERT((element & bitmask) == (element % capacity));
		return (element & bitmask);
	});
	auto ht_offsets_ptr = FlatVector::GetData<uint64_t>(ht_offsets);

	// Precompute the hash salts for faster comparison below
	UnaryExecutor::Execute<hash_t, uint16_t>(group_hashes_v, hash_salts, groups.size(),
	                                         [&](hash_t element) { return (element >> hash_prefix_shift); });
	auto hash_salts_ptr = FlatVector::GetData<uint16_t>(hash_salts);

	// Start out with all entries [0, 1, 2, ..., groups.size()]
	const SelectionVector *sel_vector = FlatVector::IncrementalSelectionVector();

	// Make a chunk that references the groups and the hashes and convert to unified format
	DataChunk group_chunk;
	group_chunk.InitializeEmpty(layout.GetTypes());
	for (idx_t grp_idx = 0; grp_idx < groups.ColumnCount(); grp_idx++) {
		group_chunk.data[grp_idx].Reference(groups.data[grp_idx]);
	}
	group_chunk.data[groups.ColumnCount()].Reference(group_hashes_v);
	group_chunk.SetCardinality(groups);
	TupleDataCollection::ToUnifiedFormat(append_state.chunk_state, group_chunk);
	auto group_data = TupleDataCollection::GetVectorData(append_state.chunk_state);

	idx_t new_group_count = 0;
	idx_t remaining_entries = groups.size();
	while (remaining_entries > 0) {
		idx_t new_entry_count = 0;
		idx_t need_compare_count = 0;
		idx_t no_match_count = 0;

		// For each remaining entry, figure out whether or not it belongs to a full or empty group
		for (idx_t i = 0; i < remaining_entries; i++) {
			const idx_t index = sel_vector->get_index(i);
			auto &ht_entry = *(((ENTRY *)this->hashes_hdl_ptr) + ht_offsets_ptr[index]);
			if (ht_entry.page_nr == 0) { // Cell is unoccupied (we use page number 0 as a "unused marker")
				D_ASSERT(group_hashes[index] >> hash_prefix_shift <= NumericLimits<uint16_t>::Maximum());
				D_ASSERT(payload_hds_ptrs.size() < NumericLimits<uint32_t>::Maximum());

				// Set page nr to 1 for now to mark it as occupied (will be corrected later) and set the salt
				ht_entry.page_nr = 1;
				ht_entry.salt = group_hashes[index] >> hash_prefix_shift;

				// Update selection lists for outer loops
				empty_vector.set_index(new_entry_count++, index);
				new_groups_out.set_index(new_group_count++, index);
			} else { // Cell is occupied: Compare salts
				if (ht_entry.salt == hash_salts_ptr[index]) {
					group_compare_vector.set_index(need_compare_count++, index);
				} else {
					no_match_vector.set_index(no_match_count++, index);
				}
			}
		}

		if (new_entry_count != 0) {
			// Append everything that belongs to an empty group
			data_collection->AppendUnified(append_state.pin_state, append_state.chunk_state, group_chunk, empty_vector,
			                               new_entry_count);
			RowOperations::InitializeStates(layout, append_state.chunk_state.row_locations,
			                                *FlatVector::IncrementalSelectionVector(), new_entry_count);

			// Get the pointers to the (possibly) newly created blocks of the data collection
			idx_t block_id = payload_hds_ptrs.empty() ? 0 : payload_hds_ptrs.size() - 1;
			UpdateBlockPointers();
			auto block_pointer = payload_hds_ptrs[block_id];
			auto block_end = block_pointer + tuples_per_block * tuple_size;

			// Set the page nrs/offsets in the 1st part of the HT now that the data has been appended
			const auto row_locations = FlatVector::GetData<data_ptr_t>(append_state.chunk_state.row_locations);
			for (idx_t new_entry_idx = 0; new_entry_idx < new_entry_count; new_entry_idx++) {
				const auto &row_location = row_locations[new_entry_idx];
				if (row_location < block_pointer || row_location > block_end) {
					block_id++;
					block_pointer = payload_hds_ptrs[block_id];
					block_end = block_pointer + tuples_per_block * tuple_size;
				}
				D_ASSERT(row_location >= block_pointer && row_location < block_end);
				D_ASSERT((row_location - block_pointer) % tuple_size == 0);
				const auto index = empty_vector.get_index(new_entry_idx);
				auto &ht_entry = *(((ENTRY *)this->hashes_hdl_ptr) + ht_offsets_ptr[index]);
				ht_entry.page_nr = block_id + 1;
				ht_entry.page_offset = (row_location - block_pointer) / tuple_size;
				addresses[index] = row_location;
			}
		}

		if (need_compare_count != 0) {
			// Get the pointers to the rows that need to be compared
			for (idx_t need_compare_idx = 0; need_compare_idx < need_compare_count; need_compare_idx++) {
				const auto index = group_compare_vector.get_index(need_compare_idx);
				const auto &ht_entry = *(((ENTRY *)this->hashes_hdl_ptr) + ht_offsets_ptr[index]);
				auto page_ptr = payload_hds_ptrs[ht_entry.page_nr - 1];
				auto page_offset = ht_entry.page_offset * tuple_size;
				addresses[index] = page_ptr + page_offset;
			}

			// Perform group comparisons
			RowOperations::Match(group_chunk, group_data.get(), layout, addresses_v, predicates, group_compare_vector,
			                     need_compare_count, &no_match_vector, no_match_count);
		}

		// Linear probing: each of the entries that do not match move to the next entry in the HT
		for (idx_t i = 0; i < no_match_count; i++) {
			idx_t index = no_match_vector.get_index(i);
			ht_offsets_ptr[index]++;
			if (ht_offsets_ptr[index] >= capacity) {
				ht_offsets_ptr[index] = 0;
			}
		}
		sel_vector = &no_match_vector;
		remaining_entries = no_match_count;
	}

	return new_group_count;
}

void GroupedAggregateHashTable::UpdateBlockPointers() {
	for (const auto &id_and_handle : append_state.pin_state.row_handles) {
		const auto &id = id_and_handle.first;
		const auto &handle = id_and_handle.second;
		if (payload_hds_ptrs.empty() || id > payload_hds_ptrs.size() - 1) {
			payload_hds_ptrs.resize(id + 1);
		}
		payload_hds_ptrs[id] = handle.Ptr();
	}
}

// this is to support distinct aggregations where we need to record whether we
// have already seen a value for a group
idx_t GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &group_hashes, Vector &addresses_out,
                                                    SelectionVector &new_groups_out) {
	switch (entry_type) {
	case HtEntryType::HT_WIDTH_64:
		return FindOrCreateGroupsInternal<aggr_ht_entry_64>(groups, group_hashes, addresses_out, new_groups_out);
	case HtEntryType::HT_WIDTH_32:
		return FindOrCreateGroupsInternal<aggr_ht_entry_32>(groups, group_hashes, addresses_out, new_groups_out);
	default:
		throw InternalException("Unknown HT entry width");
	}
}

void GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses) {
	// create a dummy new_groups sel vector
	SelectionVector new_groups(STANDARD_VECTOR_SIZE);
	FindOrCreateGroups(groups, addresses, new_groups);
}

idx_t GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses_out,
                                                    SelectionVector &new_groups_out) {
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);
	return FindOrCreateGroups(groups, hashes, addresses_out, new_groups_out);
}

struct FlushMoveState {
	explicit FlushMoveState(TupleDataCollection &collection_p)
	    : collection(collection_p), hashes(LogicalType::HASH), new_groups(STANDARD_VECTOR_SIZE),
	      group_addresses(LogicalType::POINTER), new_groups_sel(STANDARD_VECTOR_SIZE) {
		const auto &layout = collection.GetLayout();
		vector<column_t> column_ids;
		column_ids.reserve(layout.ColumnCount() - 1);
		for (idx_t col_idx = 0; col_idx < layout.ColumnCount() - 1; col_idx++) {
			column_ids.emplace_back(col_idx);
		}
		collection.InitializeScan(scan_state, column_ids, TupleDataPinProperties::DESTROY_AFTER_DONE);
		collection.InitializeScanChunk(scan_state, groups);
		hash_col_idx = layout.ColumnCount() - 1;
	}

	bool Scan();

	TupleDataCollection &collection;
	TupleDataScanState scan_state;
	DataChunk groups;
	idx_t hash_col_idx;
	Vector hashes;

	SelectionVector new_groups;
	Vector group_addresses;
	SelectionVector new_groups_sel;
};

bool FlushMoveState::Scan() {
	if (collection.Scan(scan_state, groups)) {
		collection.Gather(scan_state.chunk_state.row_locations, *FlatVector::IncrementalSelectionVector(),
		                  groups.size(), hash_col_idx, hashes, *FlatVector::IncrementalSelectionVector());
		return true;
	}
	collection.FinalizePinState(scan_state.pin_state);
	return false;
}

void GroupedAggregateHashTable::FlushMove(FlushMoveState &state) {
	FindOrCreateGroups(state.groups, state.hashes, state.group_addresses, state.new_groups_sel);
	RowOperations::CombineStates(layout, state.scan_state.chunk_state.row_locations, state.group_addresses,
	                             state.groups.size());
}

void GroupedAggregateHashTable::Combine(GroupedAggregateHashTable &other) {
	D_ASSERT(!is_finalized);

	D_ASSERT(other.layout.GetAggrWidth() == layout.GetAggrWidth());
	D_ASSERT(other.layout.GetDataWidth() == layout.GetDataWidth());
	D_ASSERT(other.layout.GetRowWidth() == layout.GetRowWidth());

	if (other.Count() == 0) {
		return;
	}

	FlushMoveState state(*other.data_collection);
	while (state.Scan()) {
		FlushMove(state);
	}

	Verify();
}

void GroupedAggregateHashTable::Partition(vector<GroupedAggregateHashTable *> &partition_hts, idx_t radix_bits) {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	D_ASSERT(partition_hts.size() == num_partitions);

	// Partition the data
	auto partitioned_data =
	    make_unique<RadixPartitionedTupleData>(buffer_manager, layout, radix_bits, layout.ColumnCount() - 1);
	partitioned_data->Partition(*data_collection);
	D_ASSERT(partitioned_data->GetPartitions().size() == num_partitions);

	// Move the partitioned data collections to the partitioned hash tables and initialize the 1st part of the HT
	auto &partitions = partitioned_data->GetPartitions();
	for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
		auto &partition_ht = *partition_hts[partition_idx];
		partition_ht.data_collection = std::move(partitions[partition_idx]);
		partition_ht.InitializeFirstPart();
	}
}

void GroupedAggregateHashTable::InitializeFirstPart() {
	// TODO: not sure if resize does the job here
	data_collection->GetBlockPointers(payload_hds_ptrs);
	switch (entry_type) {
	case HtEntryType::HT_WIDTH_64:
		Resize<aggr_ht_entry_64>(Count() * 2L);
		break;
	case HtEntryType::HT_WIDTH_32:
		Resize<aggr_ht_entry_32>(Count() * 2L);
		break;
	default:
		throw InternalException("Unknown HT entry width");
	}
}

idx_t GroupedAggregateHashTable::Scan(TupleDataParallelScanState &gstate, TupleDataLocalScanState &lstate,
                                      DataChunk &result) {
	data_collection->Scan(gstate, lstate, result);

	const auto group_cols = layout.ColumnCount() - 1;
	RowOperations::FinalizeStates(layout, lstate.scan_state.chunk_state.row_locations, result, group_cols);

	return result.size();
}

void GroupedAggregateHashTable::Finalize() {
	if (is_finalized) {
		return;
	}

	// early release hashes, not needed for partition/scan
	hashes_hdl.Destroy();
	is_finalized = true;
}

} // namespace duckdb
