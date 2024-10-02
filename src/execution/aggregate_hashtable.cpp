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
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types, vector<LogicalType> payload_types,
                                                     const vector<BoundAggregateExpression *> &bindings,
                                                     idx_t initial_capacity, idx_t radix_bits)
    : GroupedAggregateHashTable(context, allocator, std::move(group_types), std::move(payload_types),
                                AggregateObject::CreateAggregateObjects(bindings), initial_capacity, radix_bits) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types)
    : GroupedAggregateHashTable(context, allocator, std::move(group_types), {}, vector<AggregateObject>()) {
}

GroupedAggregateHashTable::AggregateHTAppendState::AggregateHTAppendState()
    : ht_offsets(LogicalType::UBIGINT), hash_salts(LogicalType::HASH), group_compare_vector(STANDARD_VECTOR_SIZE),
      no_match_vector(STANDARD_VECTOR_SIZE), empty_vector(STANDARD_VECTOR_SIZE), new_groups(STANDARD_VECTOR_SIZE),
      addresses(LogicalType::POINTER) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types_p,
                                                     vector<LogicalType> payload_types_p,
                                                     vector<AggregateObject> aggregate_objects_p,
                                                     idx_t initial_capacity, idx_t radix_bits)
    : BaseAggregateHashTable(context, allocator, aggregate_objects_p, std::move(payload_types_p)),
      radix_bits(radix_bits), count(0), capacity(0), aggregate_allocator(make_shared_ptr<ArenaAllocator>(allocator)) {

	// Append hash column to the end and initialise the row layout
	group_types_p.emplace_back(LogicalType::HASH);
	layout.Initialize(std::move(group_types_p), std::move(aggregate_objects_p));

	hash_offset = layout.GetOffsets()[layout.ColumnCount() - 1];

	// Partitioned data and pointer table
	InitializePartitionedData();
	Resize(initial_capacity);

	// Predicates
	predicates.resize(layout.ColumnCount() - 1, ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	row_matcher.Initialize(true, layout, predicates);
}

void GroupedAggregateHashTable::InitializePartitionedData() {
	if (!partitioned_data || RadixPartitioning::RadixBits(partitioned_data->PartitionCount()) != radix_bits) {
		D_ASSERT(!partitioned_data || partitioned_data->Count() == 0);
		partitioned_data =
		    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, radix_bits, layout.ColumnCount() - 1);
	} else {
		partitioned_data->Reset();
	}

	D_ASSERT(GetLayout().GetAggrWidth() == layout.GetAggrWidth());
	D_ASSERT(GetLayout().GetDataWidth() == layout.GetDataWidth());
	D_ASSERT(GetLayout().GetRowWidth() == layout.GetRowWidth());

	partitioned_data->InitializeAppendState(state.append_state, TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
}

unique_ptr<PartitionedTupleData> &GroupedAggregateHashTable::GetPartitionedData() {
	return partitioned_data;
}

shared_ptr<ArenaAllocator> GroupedAggregateHashTable::GetAggregateAllocator() {
	return aggregate_allocator;
}

GroupedAggregateHashTable::~GroupedAggregateHashTable() {
	Destroy();
}

void GroupedAggregateHashTable::Destroy() {
	if (!partitioned_data || partitioned_data->Count() == 0 || !layout.HasDestructor()) {
		return;
	}

	// There are aggregates with destructors: Call the destructor for each of the aggregates
	// Currently does not happen because aggregate destructors are called while scanning in RadixPartitionedHashTable
	// LCOV_EXCL_START
	RowOperationsState row_state(*aggregate_allocator);
	for (auto &data_collection : partitioned_data->GetPartitions()) {
		if (data_collection->Count() == 0) {
			continue;
		}
		TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::DESTROY_AFTER_DONE, false);
		auto &row_locations = iterator.GetChunkState().row_locations;
		do {
			RowOperations::DestroyStates(row_state, layout, row_locations, iterator.GetCurrentChunkCount());
		} while (iterator.Next());
		data_collection->Reset();
	}
	// LCOV_EXCL_STOP
}

const TupleDataLayout &GroupedAggregateHashTable::GetLayout() const {
	return partitioned_data->GetLayout();
}

idx_t GroupedAggregateHashTable::Count() const {
	return count;
}

idx_t GroupedAggregateHashTable::InitialCapacity() {
	return STANDARD_VECTOR_SIZE * 2ULL;
}

idx_t GroupedAggregateHashTable::GetCapacityForCount(idx_t count) {
	count = MaxValue<idx_t>(InitialCapacity(), count);
	return NextPowerOfTwo(LossyNumericCast<uint64_t>(static_cast<double>(count) * LOAD_FACTOR));
}

idx_t GroupedAggregateHashTable::Capacity() const {
	return capacity;
}

idx_t GroupedAggregateHashTable::ResizeThreshold() const {
	return LossyNumericCast<idx_t>(static_cast<double>(Capacity()) / LOAD_FACTOR);
}

idx_t GroupedAggregateHashTable::ApplyBitMask(hash_t hash) const {
	return hash & bitmask;
}

void GroupedAggregateHashTable::Verify() {
#ifdef DEBUG
	idx_t total_count = 0;
	for (idx_t i = 0; i < capacity; i++) {
		const auto &entry = entries[i];
		if (!entry.IsOccupied()) {
			continue;
		}
		auto hash = Load<hash_t>(entry.GetPointer() + hash_offset);
		D_ASSERT(entry.GetSalt() == ht_entry_t::ExtractSalt(hash));
		total_count++;
	}
	D_ASSERT(total_count == Count());
#endif
}

void GroupedAggregateHashTable::ClearPointerTable() {
	std::fill_n(entries, capacity, ht_entry_t::GetEmptyEntry());
}

void GroupedAggregateHashTable::ResetCount() {
	count = 0;
}

void GroupedAggregateHashTable::SetRadixBits(idx_t radix_bits_p) {
	radix_bits = radix_bits_p;
}

void GroupedAggregateHashTable::Resize(idx_t size) {
	D_ASSERT(size >= STANDARD_VECTOR_SIZE);
	D_ASSERT(IsPowerOfTwo(size));
	if (size < capacity) {
		throw InternalException("Cannot downsize a hash table!");
	}

	capacity = size;
	hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(ht_entry_t));
	entries = reinterpret_cast<ht_entry_t *>(hash_map.get());
	ClearPointerTable();
	bitmask = capacity - 1;

	if (Count() != 0) {
		for (auto &data_collection : partitioned_data->GetPartitions()) {
			if (data_collection->Count() == 0) {
				continue;
			}
			TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::ALREADY_PINNED, false);
			const auto row_locations = iterator.GetRowLocations();
			do {
				for (idx_t i = 0; i < iterator.GetCurrentChunkCount(); i++) {
					const auto &row_location = row_locations[i];
					const auto hash = Load<hash_t>(row_location + hash_offset);

					// Find an empty entry
					auto entry_idx = ApplyBitMask(hash);
					D_ASSERT(entry_idx == hash % capacity);
					while (entries[entry_idx].IsOccupied()) {
						entry_idx++;
						if (entry_idx >= capacity) {
							entry_idx = 0;
						}
					}
					auto &entry = entries[entry_idx];
					D_ASSERT(!entry.IsOccupied());
					entry.SetSalt(ht_entry_t::ExtractSalt(hash));
					entry.SetPointer(row_location);
					D_ASSERT(entry.IsOccupied());
				}
			} while (iterator.Next());
		}
	}

	Verify();
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload, AggregateType filter) {
	unsafe_vector<idx_t> aggregate_filter;

	auto &aggregates = layout.GetAggregates();
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		if (aggregate.aggr_type == filter) {
			aggregate_filter.push_back(i);
		}
	}
	return AddChunk(groups, payload, aggregate_filter);
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload, const unsafe_vector<idx_t> &filter) {
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);

	return AddChunk(groups, hashes, payload, filter);
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, Vector &group_hashes, DataChunk &payload,
                                          const unsafe_vector<idx_t> &filter) {
	if (groups.size() == 0) {
		return 0;
	}

#ifdef DEBUG
	D_ASSERT(groups.ColumnCount() + 1 == layout.ColumnCount());
	for (idx_t i = 0; i < groups.ColumnCount(); i++) {
		D_ASSERT(groups.GetTypes()[i] == layout.GetTypes()[i]);
	}
#endif

	const auto new_group_count = FindOrCreateGroups(groups, group_hashes, state.addresses, state.new_groups);
	VectorOperations::AddInPlace(state.addresses, NumericCast<int64_t>(layout.GetAggrOffset()), payload.size());

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
			VectorOperations::AddInPlace(state.addresses, NumericCast<int64_t>(aggr.payload_size), payload.size());
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
		VectorOperations::AddInPlace(state.addresses, NumericCast<int64_t>(aggr.payload_size), payload.size());
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
	Vector addresses(LogicalType::POINTER);
	FindOrCreateGroups(groups, addresses);
	// now fetch the aggregates
	RowOperationsState row_state(*aggregate_allocator);
	RowOperations::FinalizeStates(row_state, layout, addresses, result, 0);
}

idx_t GroupedAggregateHashTable::FindOrCreateGroupsInternal(DataChunk &groups, Vector &group_hashes_v,
                                                            Vector &addresses_v, SelectionVector &new_groups_out) {
	D_ASSERT(groups.ColumnCount() + 1 == layout.ColumnCount());
	D_ASSERT(group_hashes_v.GetType() == LogicalType::HASH);
	D_ASSERT(state.ht_offsets.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(state.ht_offsets.GetType() == LogicalType::UBIGINT);
	D_ASSERT(addresses_v.GetType() == LogicalType::POINTER);
	D_ASSERT(state.hash_salts.GetType() == LogicalType::HASH);

	// Need to fit the entire vector, and resize at threshold
	if (Count() + groups.size() > capacity || Count() + groups.size() > ResizeThreshold()) {
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
	const auto hash_salts = FlatVector::GetData<hash_t>(state.hash_salts);
	for (idx_t r = 0; r < groups.size(); r++) {
		const auto &hash = hashes[r];
		ht_offsets[r] = ApplyBitMask(hash);
		D_ASSERT(ht_offsets[r] == hash % capacity);
		hash_salts[r] = ht_entry_t::ExtractSalt(hash);
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
	auto &chunk_state = state.append_state.chunk_state;
	TupleDataCollection::ToUnifiedFormat(chunk_state, state.group_chunk);
	if (!state.group_data) {
		state.group_data = make_unsafe_uniq_array_uninitialized<UnifiedVectorFormat>(state.group_chunk.ColumnCount());
	}
	TupleDataCollection::GetVectorData(chunk_state, state.group_data.get());

	idx_t new_group_count = 0;
	idx_t remaining_entries = groups.size();
	idx_t iteration_count;
	for (iteration_count = 0; remaining_entries > 0 && iteration_count < capacity; iteration_count++) {
		idx_t new_entry_count = 0;
		idx_t need_compare_count = 0;
		idx_t no_match_count = 0;

		// For each remaining entry, figure out whether or not it belongs to a full or empty group
		for (idx_t i = 0; i < remaining_entries; i++) {
			const auto index = sel_vector->get_index(i);
			const auto &salt = hash_salts[index];
			auto &ht_offset = ht_offsets[index];

			idx_t inner_iteration_count;
			for (inner_iteration_count = 0; inner_iteration_count < capacity; inner_iteration_count++) {
				auto &entry = entries[ht_offset];
				if (entry.IsOccupied()) { // Cell is occupied: Compare salts
					if (entry.GetSalt() == salt) {
						// Same salt, compare group keys
						state.group_compare_vector.set_index(need_compare_count++, index);
						break;
					}
					// Different salts, move to next entry (linear probing)
					IncrementAndWrap(ht_offset, bitmask);
				} else { // Cell is unoccupied, let's claim it
					// Set salt (also marks as occupied)
					entry.SetSalt(salt);
					// Update selection lists for outer loops
					state.empty_vector.set_index(new_entry_count++, index);
					new_groups_out.set_index(new_group_count++, index);
					break;
				}
			}
			if (inner_iteration_count == capacity) {
				throw InternalException("Maximum inner iteration count reached in GroupedAggregateHashTable");
			}
		}

		if (new_entry_count != 0) {
			// Append everything that belongs to an empty group
			partitioned_data->AppendUnified(state.append_state, state.group_chunk, state.empty_vector, new_entry_count);
			RowOperations::InitializeStates(layout, chunk_state.row_locations,
			                                *FlatVector::IncrementalSelectionVector(), new_entry_count);

			// Set the entry pointers in the 1st part of the HT now that the data has been appended
			const auto row_locations = FlatVector::GetData<data_ptr_t>(chunk_state.row_locations);
			const auto &row_sel = state.append_state.reverse_partition_sel;
			for (idx_t new_entry_idx = 0; new_entry_idx < new_entry_count; new_entry_idx++) {
				const auto index = state.empty_vector.get_index(new_entry_idx);
				const auto row_idx = row_sel.get_index(index);
				const auto &row_location = row_locations[row_idx];

				auto &entry = entries[ht_offsets[index]];

				entry.SetPointer(row_location);
				addresses[index] = row_location;
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
			row_matcher.Match(state.group_chunk, chunk_state.vector_data, state.group_compare_vector,
			                  need_compare_count, layout, addresses_v, &state.no_match_vector, no_match_count);
		}

		// Linear probing: each of the entries that do not match move to the next entry in the HT
		for (idx_t i = 0; i < no_match_count; i++) {
			const auto index = state.no_match_vector.get_index(i);
			auto &ht_offset = ht_offsets[index];
			IncrementAndWrap(ht_offset, bitmask);
		}
		sel_vector = &state.no_match_vector;
		remaining_entries = no_match_count;
	}
	if (iteration_count == capacity) {
		throw InternalException("Maximum outer iteration count reached in GroupedAggregateHashTable");
	}

	count += new_group_count;
	return new_group_count;
}

// this is to support distinct aggregations where we need to record whether we
// have already seen a value for a group
idx_t GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &group_hashes, Vector &addresses_out,
                                                    SelectionVector &new_groups_out) {
	return FindOrCreateGroupsInternal(groups, group_hashes, addresses_out, new_groups_out);
}

void GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses) {
	// create a dummy new_groups sel vector
	FindOrCreateGroups(groups, addresses, state.new_groups);
}

idx_t GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses_out,
                                                    SelectionVector &new_groups_out) {
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);
	return FindOrCreateGroups(groups, hashes, addresses_out, new_groups_out);
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
		collection.InitializeScan(scan_state, column_ids, TupleDataPinProperties::DESTROY_AFTER_DONE);
		collection.InitializeScanChunk(scan_state, groups);
		hash_col_idx = layout.ColumnCount() - 1;
	}

	bool Scan() {
		if (collection.Scan(scan_state, groups)) {
			collection.Gather(scan_state.chunk_state.row_locations, *FlatVector::IncrementalSelectionVector(),
			                  groups.size(), hash_col_idx, hashes, *FlatVector::IncrementalSelectionVector(), nullptr);
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

	Vector group_addresses;
	SelectionVector new_groups_sel;
};

void GroupedAggregateHashTable::Combine(GroupedAggregateHashTable &other) {
	auto other_data = other.partitioned_data->GetUnpartitioned();
	Combine(*other_data);

	// Inherit ownership to all stored aggregate allocators
	stored_allocators.emplace_back(other.aggregate_allocator);
	for (const auto &stored_allocator : other.stored_allocators) {
		stored_allocators.emplace_back(stored_allocator);
	}
}

void GroupedAggregateHashTable::Combine(TupleDataCollection &other_data, optional_ptr<atomic<double>> progress) {
	D_ASSERT(other_data.GetLayout().GetAggrWidth() == layout.GetAggrWidth());
	D_ASSERT(other_data.GetLayout().GetDataWidth() == layout.GetDataWidth());
	D_ASSERT(other_data.GetLayout().GetRowWidth() == layout.GetRowWidth());

	if (other_data.Count() == 0) {
		return;
	}

	FlushMoveState fm_state(other_data);
	RowOperationsState row_state(*aggregate_allocator);

	idx_t chunk_idx = 0;
	const auto chunk_count = other_data.ChunkCount();
	while (fm_state.Scan()) {
		FindOrCreateGroups(fm_state.groups, fm_state.hashes, fm_state.group_addresses, fm_state.new_groups_sel);
		RowOperations::CombineStates(row_state, layout, fm_state.scan_state.chunk_state.row_locations,
		                             fm_state.group_addresses, fm_state.groups.size());
		if (layout.HasDestructor()) {
			RowOperations::DestroyStates(row_state, layout, fm_state.scan_state.chunk_state.row_locations,
			                             fm_state.groups.size());
		}

		if (progress) {
			*progress = double(++chunk_idx) / double(chunk_count);
		}
	}

	Verify();
}

void GroupedAggregateHashTable::UnpinData() {
	partitioned_data->FlushAppendState(state.append_state);
	partitioned_data->Unpin();
}

} // namespace duckdb
