#include "duckdb/execution/aggregate_hashtable.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types, vector<LogicalType> payload_types,
                                                     const vector<BoundAggregateExpression *> &bindings,
                                                     idx_t initial_capacity)
    : GroupedAggregateHashTable(context, allocator, std::move(group_types), std::move(payload_types),
                                AggregateObject::CreateAggregateObjects(bindings), initial_capacity) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types)
    : GroupedAggregateHashTable(context, allocator, std::move(group_types), {}, vector<AggregateObject>()) {
}

AggregateHTAppendState::AggregateHTAppendState()
    : ht_offsets(LogicalTypeId::BIGINT), hash_salts(LogicalTypeId::SMALLINT),
      group_compare_vector(STANDARD_VECTOR_SIZE), no_match_vector(STANDARD_VECTOR_SIZE),
      empty_vector(STANDARD_VECTOR_SIZE), new_groups(STANDARD_VECTOR_SIZE), addresses(LogicalType::POINTER),
      chunk_state_initialized(false) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types_p,
                                                     vector<LogicalType> payload_types_p,
                                                     vector<AggregateObject> aggregate_objects_p,
                                                     idx_t initial_capacity)
    : BaseAggregateHashTable(context, allocator, aggregate_objects_p, std::move(payload_types_p)), capacity(0),
      aggregate_allocator(make_shared<ArenaAllocator>(allocator)), is_finalized(false) {
	// Append hash column to the end and initialise the row layout
	group_types_p.emplace_back(LogicalType::HASH);
	layout.Initialize(std::move(group_types_p), std::move(aggregate_objects_p));

	// HT layout
	hash_offset = layout.GetOffsets()[layout.ColumnCount() - 1];
	data_collection = make_uniq<TupleDataCollection>(buffer_manager, layout);
	data_collection->InitializeAppend(td_pin_state, TupleDataPinProperties::KEEP_EVERYTHING_PINNED);

	Resize(initial_capacity);

	predicates.resize(layout.ColumnCount() - 1, ExpressionType::COMPARE_EQUAL);
}

GroupedAggregateHashTable::~GroupedAggregateHashTable() {
	Destroy();
}

void GroupedAggregateHashTable::Destroy() {
	if (data_collection->Count() == 0) {
		return;
	}

	// Check if there is an aggregate with a destructor
	bool has_destructor = false;
	for (auto &aggr : layout.GetAggregates()) {
		if (aggr.function.destructor) {
			has_destructor = true;
		}
	}
	if (!has_destructor) {
		return;
	}

	// There are aggregates with destructors: Call the destructor for each of the aggregates
	RowOperationsState state(*aggregate_allocator);
	TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::DESTROY_AFTER_DONE, false);
	auto &row_locations = iterator.GetChunkState().row_locations;
	do {
		RowOperations::DestroyStates(state, layout, row_locations, iterator.GetCurrentChunkCount());
	} while (iterator.Next());
	data_collection->Reset();
}

idx_t GroupedAggregateHashTable::Count() const {
	return data_collection->Count();
}

idx_t GroupedAggregateHashTable::InitialCapacity() {
	return STANDARD_VECTOR_SIZE * 2ULL;
}

idx_t GroupedAggregateHashTable::SinkCapacity() {
	return FIRST_PART_SINK_SIZE / sizeof(aggr_ht_entry_t);
}

idx_t GroupedAggregateHashTable::Capacity() const {
	return capacity;
}

idx_t GroupedAggregateHashTable::ResizeThreshold() const {
	return capacity / LOAD_FACTOR;
}

TupleDataCollection &GroupedAggregateHashTable::GetDataCollection() {
	return *data_collection;
}

idx_t GroupedAggregateHashTable::DataSize() const {
	return data_collection->SizeInBytes();
}

idx_t GroupedAggregateHashTable::FirstPartSize(idx_t count) {
	return NextPowerOfTwo(count * 2L) * sizeof(aggr_ht_entry_t);
}

idx_t GroupedAggregateHashTable::TotalSize() const {
	return DataSize() + FirstPartSize(Count());
}

idx_t GroupedAggregateHashTable::ApplyBitMask(hash_t hash) const {
	return hash & bitmask;
}

void GroupedAggregateHashTable::Verify() {
	idx_t count = 0;
	for (idx_t i = 0; i < capacity; i++) {
		const auto &entry = entries[i];
		if (!entry.IsOccupied()) {
			continue;
		}
		auto hash = Load<hash_t>(entry.GetPointer() + hash_offset);
		D_ASSERT((entry.GetSalt()) == aggr_ht_entry_t::ExtractSalt(hash));
		count++;
	}
	D_ASSERT(count == Count());
}

void GroupedAggregateHashTable::Resize(idx_t size) {
	D_ASSERT(!is_finalized);
	D_ASSERT(size >= STANDARD_VECTOR_SIZE);
	D_ASSERT(IsPowerOfTwo(size));
	if (size < capacity) {
		throw InternalException("Cannot downsize a hash table!");
	}

	capacity = size;
	hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(aggr_ht_entry_t));
	entries = reinterpret_cast<aggr_ht_entry_t *>(hash_map.get());
	std::fill_n(entries, capacity, aggr_ht_entry_t(0));
	bitmask = capacity - 1;

	if (Count() != 0) {
		TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::ALREADY_PINNED, false);
		const auto row_locations = iterator.GetRowLocations();
		do {
			for (idx_t i = 0; i < iterator.GetCurrentChunkCount(); i++) {
				const auto &row_location = row_locations[i];
				const auto hash = Load<hash_t>(row_location + hash_offset);

				// Find an empty entry
				auto entry_idx = ApplyBitMask(hash);
				D_ASSERT(entry_idx == hash % capacity);
				while (entries[entry_idx].IsOccupied() > 0) {
					entry_idx++;
					if (entry_idx >= capacity) {
						entry_idx = 0;
					}
				}
				auto &entry = entries[entry_idx];
				D_ASSERT(!entry.IsOccupied());
				entry.SetSaltOverwrite(hash);
				entry.SetPointer(row_location);
				D_ASSERT(entry.IsOccupied());
			}
		} while (iterator.Next());
	}

	Verify();
}

idx_t GroupedAggregateHashTable::AddChunk(AggregateHTAppendState &state, DataChunk &groups, DataChunk &payload,
                                          AggregateType filter) {
	unsafe_vector<idx_t> aggregate_filter;

	auto &aggregates = layout.GetAggregates();
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		if (aggregate.aggr_type == filter) {
			aggregate_filter.push_back(i);
		}
	}
	return AddChunk(state, groups, payload, aggregate_filter);
}

idx_t GroupedAggregateHashTable::AddChunk(AggregateHTAppendState &state, DataChunk &groups, DataChunk &payload,
                                          const unsafe_vector<idx_t> &filter) {
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);

	return AddChunk(state, groups, hashes, payload, filter);
}

idx_t GroupedAggregateHashTable::AddChunk(AggregateHTAppendState &state, DataChunk &groups, Vector &group_hashes,
                                          DataChunk &payload, const unsafe_vector<idx_t> &filter) {
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

	const auto new_group_count = FindOrCreateGroups(state, groups, group_hashes, state.addresses, state.new_groups);
	VectorOperations::AddInPlace(state.addresses, layout.GetAggrOffset(), payload.size());

	// Now every cell has an entry, update the aggregates
	auto &aggregates = layout.GetAggregates();
	idx_t filter_idx = 0;
	idx_t payload_idx = 0;
	RowOperationsState row_state(*aggregate_allocator);
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i];
		if (filter_idx >= filter.size() || i < filter[filter_idx]) {
			// Skip all the aggregates that are not in the filter
			payload_idx += aggr.child_count;
			VectorOperations::AddInPlace(state.addresses, aggr.payload_size, payload.size());
			continue;
		}
		D_ASSERT(i == filter[filter_idx]);

		if (aggr.aggr_type != AggregateType::DISTINCT && aggr.filter) {
			RowOperations::UpdateFilteredStates(row_state, filter_set.GetFilterData(i), aggr, state.addresses, payload,
			                                    payload_idx);
		} else {
			RowOperations::UpdateStates(row_state, aggr, state.addresses, payload, payload_idx, payload.size());
		}

		// Move to the next aggregate
		payload_idx += aggr.child_count;
		VectorOperations::AddInPlace(state.addresses, aggr.payload_size, payload.size());
		filter_idx++;
	}

	Verify();
	return new_group_count;
}

void GroupedAggregateHashTable::FetchAggregates(DataChunk &groups, DataChunk &result) {
#ifdef DEBUG
	groups.Verify();
	D_ASSERT(groups.ColumnCount() + 1 == layout.ColumnCount());
	for (idx_t i = 0; i < result.ColumnCount(); i++) {
		D_ASSERT(result.data[i].GetType() == payload_types[i]);
	}
#endif

	result.SetCardinality(groups);
	if (groups.size() == 0) {
		return;
	}

	// find the groups associated with the addresses
	// FIXME: this should not use the FindOrCreateGroups, creating them is unnecessary
	AggregateHTAppendState append_state;
	Vector addresses(LogicalType::POINTER);
	FindOrCreateGroups(append_state, groups, addresses);
	// now fetch the aggregates
	RowOperationsState row_state(*aggregate_allocator);
	RowOperations::FinalizeStates(row_state, layout, addresses, result, 0);
}

idx_t GroupedAggregateHashTable::FindOrCreateGroupsInternal(AggregateHTAppendState &state, DataChunk &groups,
                                                            Vector &group_hashes_v, Vector &addresses_v,
                                                            SelectionVector &new_groups_out) {
	D_ASSERT(!is_finalized);
	D_ASSERT(groups.ColumnCount() + 1 == layout.ColumnCount());
	D_ASSERT(group_hashes_v.GetType() == LogicalType::HASH);
	D_ASSERT(state.ht_offsets.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(state.ht_offsets.GetType() == LogicalType::BIGINT);
	D_ASSERT(addresses_v.GetType() == LogicalType::POINTER);
	D_ASSERT(state.hash_salts.GetType() == LogicalType::SMALLINT);

	// Resize at 50% capacity, also need to fit the entire vector
	if (capacity - Count() <= groups.size() || Count() > ResizeThreshold()) {
		Verify();
		Resize(capacity * 2);
	}
	D_ASSERT(capacity - Count() >= groups.size()); // we need to be able to fit at least one vector of data

	group_hashes_v.Flatten(groups.size());
	auto hashes = FlatVector::GetData<hash_t>(group_hashes_v);

	addresses_v.Flatten(groups.size());
	auto addresses = FlatVector::GetData<data_ptr_t>(addresses_v);

	// Compute the entry in the table based on the hash using a modulo,
	// and precompute the hash salts for faster comparison below
	auto ht_offsets = FlatVector::GetData<uint64_t>(state.ht_offsets);
	auto hash_salts = FlatVector::GetData<uint16_t>(state.hash_salts);
	for (idx_t r = 0; r < groups.size(); r++) {
		const auto &hash = hashes[r];
		ht_offsets[r] = ApplyBitMask(hash);
		D_ASSERT(ht_offsets[r] == hash % capacity);
		hash_salts[r] = aggr_ht_entry_t::ExtractSalt(hash);
	}

	// we start out with all entries [0, 1, 2, ..., groups.size()]
	const SelectionVector *sel_vector = FlatVector::IncrementalSelectionVector();

	// Make a chunk that references the groups and the hashes and convert to unified format
	if (state.group_chunk.ColumnCount() == 0) {
		state.group_chunk.InitializeEmpty(layout.GetTypes());
	}
	D_ASSERT(state.group_chunk.ColumnCount() == layout.GetTypes().size());
	for (idx_t grp_idx = 0; grp_idx < groups.ColumnCount(); grp_idx++) {
		state.group_chunk.data[grp_idx].Reference(groups.data[grp_idx]);
	}
	state.group_chunk.data[groups.ColumnCount()].Reference(group_hashes_v);
	state.group_chunk.SetCardinality(groups);

	// convert all vectors to unified format
	if (!state.chunk_state_initialized) {
		data_collection->InitializeAppend(state.chunk_state);
		state.chunk_state_initialized = true;
	}
	TupleDataCollection::ToUnifiedFormat(state.chunk_state, state.group_chunk);
	if (!state.group_data) {
		state.group_data = make_unsafe_uniq_array<UnifiedVectorFormat>(state.group_chunk.ColumnCount());
	}
	TupleDataCollection::GetVectorData(state.chunk_state, state.group_data.get());

	idx_t new_group_count = 0;
	idx_t remaining_entries = groups.size();
	while (remaining_entries > 0) {
		idx_t new_entry_count = 0;
		idx_t need_compare_count = 0;
		idx_t no_match_count = 0;

		// For each remaining entry, figure out whether or not it belongs to a full or empty group
		for (idx_t i = 0; i < remaining_entries; i++) {
			const auto index = sel_vector->get_index(i);
			const auto &salt = hash_salts[index];
			auto &entry = entries[ht_offsets[index]];
			if (entry.IsOccupied()) { // Cell is occupied: Compare salts
				if (entry.GetSalt() == salt) {
					state.group_compare_vector.set_index(need_compare_count++, index);
				} else {
					state.no_match_vector.set_index(no_match_count++, index);
				}
			} else { // Cell is unoccupied
				// Set salt and mark as occupied
				entry.SetSalt(salt);
				entry.SetOccupied();

				// Update selection lists for outer loops
				state.empty_vector.set_index(new_entry_count++, index);
				new_groups_out.set_index(new_group_count++, index);
			}
		}

		if (new_entry_count != 0) {
			// Append everything that belongs to an empty group
			data_collection->AppendUnified(td_pin_state, state.chunk_state, state.group_chunk, state.empty_vector,
			                               new_entry_count);
			RowOperations::InitializeStates(layout, state.chunk_state.row_locations,
			                                *FlatVector::IncrementalSelectionVector(), new_entry_count);

			// Set the page nrs/offsets in the 1st part of the HT now that the data has been appended
			const auto row_locations = FlatVector::GetData<data_ptr_t>(state.chunk_state.row_locations);
			for (idx_t new_entry_idx = 0; new_entry_idx < new_entry_count; new_entry_idx++) {
				const auto index = state.empty_vector.get_index(new_entry_idx);
				auto &entry = entries[ht_offsets[index]];
				entry.SetPointer(row_locations[new_entry_idx]);
			}
		}

		if (need_compare_count != 0) {
			// Get the pointers to the rows that need to be compared
			for (idx_t need_compare_idx = 0; need_compare_idx < need_compare_count; need_compare_idx++) {
				const auto index = state.group_compare_vector.get_index(need_compare_idx);
				const auto &entry = entries[ht_offsets[index]];
				addresses[index] = entry.GetPointer();
			}

			// Perform group comparisons
			RowOperations::Match(state.group_chunk, state.group_data.get(), layout, addresses_v, predicates,
			                     state.group_compare_vector, need_compare_count, &state.no_match_vector,
			                     no_match_count);
		}

		// Linear probing: each of the entries that do not match move to the next entry in the HT
		for (idx_t i = 0; i < no_match_count; i++) {
			idx_t index = state.no_match_vector.get_index(i);
			ht_offsets[index]++;
			if (ht_offsets[index] >= capacity) {
				ht_offsets[index] = 0;
			}
		}
		sel_vector = &state.no_match_vector;
		remaining_entries = no_match_count;
	}

	return new_group_count;
}

// this is to support distinct aggregations where we need to record whether we
// have already seen a value for a group
idx_t GroupedAggregateHashTable::FindOrCreateGroups(AggregateHTAppendState &state, DataChunk &groups,
                                                    Vector &group_hashes, Vector &addresses_out,
                                                    SelectionVector &new_groups_out) {
	return FindOrCreateGroupsInternal(state, groups, group_hashes, addresses_out, new_groups_out);
}

void GroupedAggregateHashTable::FindOrCreateGroups(AggregateHTAppendState &state, DataChunk &groups,
                                                   Vector &addresses) {
	// create a dummy new_groups sel vector
	FindOrCreateGroups(state, groups, addresses, state.new_groups);
}

idx_t GroupedAggregateHashTable::FindOrCreateGroups(AggregateHTAppendState &state, DataChunk &groups,
                                                    Vector &addresses_out, SelectionVector &new_groups_out) {
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);
	return FindOrCreateGroups(state, groups, hashes, addresses_out, new_groups_out);
}

struct FlushMoveState {
	explicit FlushMoveState(TupleDataCollection &collection_p)
	    : collection(collection_p), hashes(LogicalType::HASH), group_addresses(LogicalType::POINTER),
	      new_groups_sel(STANDARD_VECTOR_SIZE) {
		const auto &layout = collection.GetLayout();
		vector<column_t> column_ids;
		column_ids.reserve(layout.ColumnCount() - 1);
		for (idx_t col_idx = 0; col_idx < layout.ColumnCount() - 1; col_idx++) {
			column_ids.emplace_back(col_idx);
		}
		// FIXME DESTROY_AFTER_DONE if we make it possible to pass a selection vector to RowOperations::DestroyStates?
		collection.InitializeScan(scan_state, column_ids, TupleDataPinProperties::UNPIN_AFTER_DONE);
		collection.InitializeScanChunk(scan_state, groups);
		hash_col_idx = layout.ColumnCount() - 1;
	}

	bool Scan() {
		if (collection.Scan(scan_state, groups)) {
			collection.Gather(scan_state.chunk_state.row_locations, *FlatVector::IncrementalSelectionVector(),
			                  groups.size(), hash_col_idx, hashes, *FlatVector::IncrementalSelectionVector());
			return true;
		}

		collection.FinalizePinState(scan_state.pin_state);
		return false;
	}

	TupleDataCollection &collection;
	TupleDataScanState scan_state;
	DataChunk groups;

	idx_t hash_col_idx;
	Vector hashes;

	AggregateHTAppendState append_state;
	Vector group_addresses;
	SelectionVector new_groups_sel;
};

void GroupedAggregateHashTable::Combine(GroupedAggregateHashTable &other) {
	D_ASSERT(!is_finalized);

	D_ASSERT(other.layout.GetAggrWidth() == layout.GetAggrWidth());
	D_ASSERT(other.layout.GetDataWidth() == layout.GetDataWidth());
	D_ASSERT(other.layout.GetRowWidth() == layout.GetRowWidth());

	if (other.Count() == 0) {
		return;
	}

	FlushMoveState state(*other.data_collection);
	RowOperationsState row_state(*aggregate_allocator);
	while (state.Scan()) {
		FindOrCreateGroups(state.append_state, state.groups, state.hashes, state.group_addresses, state.new_groups_sel);
		RowOperations::CombineStates(row_state, layout, state.scan_state.chunk_state.row_locations,
		                             state.group_addresses, state.groups.size());
	}

	Verify();
}

void GroupedAggregateHashTable::Append(GroupedAggregateHashTable &other) {
	data_collection->Combine(other.GetDataCollection());

	// Inherit ownership to all stored aggregate allocators
	stored_allocators.emplace_back(other.aggregate_allocator);
	for (const auto &stored_allocator : other.stored_allocators) {
		stored_allocators.emplace_back(stored_allocator);
	}
}

void GroupedAggregateHashTable::Partition(vector<GroupedAggregateHashTable *> &partition_hts, idx_t radix_bits,
                                          bool sink_done) {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	D_ASSERT(partition_hts.size() == num_partitions);

	// Partition the data
	auto pin_properties =
	    sink_done ? TupleDataPinProperties::UNPIN_AFTER_DONE : TupleDataPinProperties::KEEP_EVERYTHING_PINNED;
	auto partitioned_data =
	    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, radix_bits, layout.ColumnCount() - 1);
	partitioned_data->Partition(*data_collection, pin_properties);
	D_ASSERT(partitioned_data->GetPartitions().size() == num_partitions);

	// Move the partitioned data collections to the partitioned hash tables and initialize the 1st part of the HT
	auto &partitions = partitioned_data->GetPartitions();
	for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
		auto &partition_ht = *partition_hts[partition_idx];
		partition_ht.data_collection = std::move(partitions[partition_idx]);

		// Inherit ownership to all stored aggregate allocators
		partition_ht.stored_allocators.emplace_back(aggregate_allocator);
		for (const auto &stored_allocator : stored_allocators) {
			partition_ht.stored_allocators.emplace_back(stored_allocator);
		}

		if (!sink_done) {
			partition_ht.InitializeFirstPart();
			partition_ht.Verify();
		}
	}
}

void GroupedAggregateHashTable::InitializeFirstPart() {
	auto size = MaxValue<idx_t>(NextPowerOfTwo(Count() * 2L), capacity);
	Resize(size);
}

idx_t GroupedAggregateHashTable::Scan(TupleDataParallelScanState &gstate, TupleDataLocalScanState &lstate,
                                      DataChunk &result) {
	data_collection->Scan(gstate, lstate, result);

	RowOperationsState row_state(*aggregate_allocator);
	const auto group_cols = layout.ColumnCount() - 1;
	RowOperations::FinalizeStates(row_state, layout, lstate.chunk_state.row_locations, result, group_cols);

	return result.size();
}

void GroupedAggregateHashTable::Finalize() {
	if (is_finalized) {
		return;
	}

	// Early release hashes (not needed for partition/scan) and data collection (will be pinned again when scanning)
	hash_map.Reset();
	data_collection->FinalizePinState(td_pin_state);
	data_collection->Unpin();

	is_finalized = true;
}

} // namespace duckdb
