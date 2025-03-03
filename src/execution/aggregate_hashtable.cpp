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

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context_p, Allocator &allocator,
                                                     vector<LogicalType> group_types_p,
                                                     vector<LogicalType> payload_types_p,
                                                     vector<AggregateObject> aggregate_objects_p,
                                                     idx_t initial_capacity, idx_t radix_bits)
    : BaseAggregateHashTable(context_p, allocator, aggregate_objects_p, std::move(payload_types_p)), context(context_p),
      radix_bits(radix_bits), count(0), capacity(0), skip_lookups(false),
      aggregate_allocator(make_shared_ptr<ArenaAllocator>(allocator)) {

	// Append hash column to the end and initialise the row layout
	group_types_p.emplace_back(LogicalType::HASH);
	layout.Initialize(std::move(group_types_p), std::move(aggregate_objects_p));

	hash_offset = layout.GetOffsets()[layout.ColumnCount() - 1];

	// Partitioned data and pointer table
	InitializePartitionedData();
	if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD) {
		InitializeUnpartitionedData();
	}
	Resize(initial_capacity);

	// Predicates
	predicates.resize(layout.ColumnCount() - 1, ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	row_matcher.Initialize(true, layout, predicates);
}

void GroupedAggregateHashTable::InitializePartitionedData() {
	if (!partitioned_data ||
	    RadixPartitioning::RadixBitsOfPowerOfTwo(partitioned_data->PartitionCount()) != radix_bits) {
		D_ASSERT(!partitioned_data || partitioned_data->Count() == 0);
		partitioned_data =
		    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, radix_bits, layout.ColumnCount() - 1);
	} else {
		partitioned_data->Reset();
	}

	D_ASSERT(GetLayout().GetAggrWidth() == layout.GetAggrWidth());
	D_ASSERT(GetLayout().GetDataWidth() == layout.GetDataWidth());
	D_ASSERT(GetLayout().GetRowWidth() == layout.GetRowWidth());

	partitioned_data->InitializeAppendState(state.partitioned_append_state,
	                                        TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
}

void GroupedAggregateHashTable::InitializeUnpartitionedData() {
	D_ASSERT(radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD);
	if (!unpartitioned_data) {
		unpartitioned_data =
		    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, 0ULL, layout.ColumnCount() - 1);
	} else {
		unpartitioned_data->Reset();
	}
	unpartitioned_data->InitializeAppendState(state.unpartitioned_append_state,
	                                          TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
}

const PartitionedTupleData &GroupedAggregateHashTable::GetPartitionedData() const {
	return *partitioned_data;
}

unique_ptr<PartitionedTupleData> GroupedAggregateHashTable::AcquirePartitionedData() {
	// Flush/unpin partitioned data
	partitioned_data->FlushAppendState(state.partitioned_append_state);
	partitioned_data->Unpin();

	if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD) {
		// Flush/unpin unpartitioned data and append to partitioned data
		if (unpartitioned_data) {
			unpartitioned_data->FlushAppendState(state.unpartitioned_append_state);
			unpartitioned_data->Unpin();
			unpartitioned_data->Repartition(context, *partitioned_data);
		}
		InitializeUnpartitionedData();
	}

	// Return and re-initialize
	auto result = std::move(partitioned_data);
	InitializePartitionedData();
	return result;
}

void GroupedAggregateHashTable::Abandon() {
	if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD) {
		// Flush/unpin unpartitioned data and append to partitioned data
		if (unpartitioned_data) {
			unpartitioned_data->FlushAppendState(state.unpartitioned_append_state);
			unpartitioned_data->Unpin();
			unpartitioned_data->Repartition(context, *partitioned_data);
		}
		InitializeUnpartitionedData();
	}

	// Start over
	ClearPointerTable();
	count = 0;

	// Resetting the id ensures the dict state is reset properly when needed
	state.dict_state.dictionary_id = string();
}

void GroupedAggregateHashTable::Repartition() {
	auto old = AcquirePartitionedData();
	D_ASSERT(old->GetPartitions().size() != partitioned_data->GetPartitions().size());
	old->Repartition(context, *partitioned_data);
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
	return ResizeThreshold(Capacity());
}

idx_t GroupedAggregateHashTable::ResizeThreshold(const idx_t capacity) {
	return LossyNumericCast<idx_t>(static_cast<double>(capacity) / LOAD_FACTOR);
}

idx_t GroupedAggregateHashTable::ApplyBitMask(hash_t hash) const {
	return hash & bitmask;
}

void GroupedAggregateHashTable::Verify() {
#ifdef DEBUG
	if (skip_lookups) {
		return;
	}
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
	std::fill_n(entries, capacity, ht_entry_t());
}

void GroupedAggregateHashTable::SetRadixBits(idx_t radix_bits_p) {
	radix_bits = radix_bits_p;
}

idx_t GroupedAggregateHashTable::GetRadixBits() const {
	return radix_bits;
}

idx_t GroupedAggregateHashTable::GetSinkCount() const {
	return sink_count;
}

void GroupedAggregateHashTable::SkipLookups() {
	skip_lookups = true;
}

void GroupedAggregateHashTable::Resize(idx_t size) {
	D_ASSERT(size >= STANDARD_VECTOR_SIZE);
	D_ASSERT(IsPowerOfTwo(size));
	if (Count() != 0 && size < capacity) {
		throw InternalException("Cannot downsize a non-empty hash table!");
	}

	capacity = size;
	hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(ht_entry_t));
	entries = reinterpret_cast<ht_entry_t *>(hash_map.get());
	ClearPointerTable();
	bitmask = capacity - 1;

	if (Count() != 0) {
		ReinsertTuples(*partitioned_data);
		if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD) {
			ReinsertTuples(*unpartitioned_data);
		}
	}

	Verify();
}

void GroupedAggregateHashTable::ReinsertTuples(PartitionedTupleData &data) {
	for (auto &data_collection : data.GetPartitions()) {
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
				auto ht_offset = ApplyBitMask(hash);
				D_ASSERT(ht_offset == hash % capacity);
				while (entries[ht_offset].IsOccupied()) {
					IncrementAndWrap(ht_offset, bitmask);
				}
				auto &entry = entries[ht_offset];
				D_ASSERT(!entry.IsOccupied());
				entry.SetSalt(ht_entry_t::ExtractSalt(hash));
				entry.SetPointer(row_location);
				D_ASSERT(entry.IsOccupied());
			}
		} while (iterator.Next());
	}
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

GroupedAggregateHashTable::AggregateDictionaryState::AggregateDictionaryState()
    : hashes(LogicalType::HASH), new_dictionary_pointers(LogicalType::POINTER), unique_entries(STANDARD_VECTOR_SIZE) {
}

optional_idx GroupedAggregateHashTable::TryAddDictionaryGroups(DataChunk &groups, DataChunk &payload,
                                                               const unsafe_vector<idx_t> &filter) {
	static constexpr idx_t MAX_DICTIONARY_SIZE_THRESHOLD = 20000;
	static constexpr idx_t DICTIONARY_THRESHOLD = 2;
	// dictionary vector - check if this is a duplicate eliminated dictionary from the storage
	auto &dict_col = groups.data[0];
	auto opt_dict_size = DictionaryVector::DictionarySize(dict_col);
	if (!opt_dict_size.IsValid()) {
		// dict size not known - this is not a dictionary that comes from the storage
		return optional_idx();
	}
	idx_t dict_size = opt_dict_size.GetIndex();
	auto &dictionary_id = DictionaryVector::DictionaryId(dict_col);
	if (dictionary_id.empty()) {
		// dictionary has no id, we can't cache across vectors
		// only use dictionary compression if there are fewer entries than groups
		if (dict_size * DICTIONARY_THRESHOLD >= groups.size()) {
			// dictionary is too large - use regular aggregation
			return optional_idx();
		}
	} else {
		// dictionary has an id - we can cache across vectors
		// use a much larger limit for dictionary
		if (dict_size >= MAX_DICTIONARY_SIZE_THRESHOLD) {
			// dictionary is too large - use regular aggregation
			return optional_idx();
		}
	}
	auto &dictionary_vector = DictionaryVector::Child(dict_col);
	auto &offsets = DictionaryVector::SelVector(dict_col);
	auto &dict_state = state.dict_state;
	if (dict_state.dictionary_id.empty() || dict_state.dictionary_id != dictionary_id) {
		// new dictionary - initialize the index state
		if (dict_size > dict_state.capacity) {
			dict_state.dictionary_addresses = make_uniq<Vector>(LogicalType::POINTER, dict_size);
			dict_state.found_entry = make_unsafe_uniq_array<bool>(dict_size);
			dict_state.capacity = dict_size;
		}
		memset(dict_state.found_entry.get(), 0, dict_size * sizeof(bool));
		dict_state.dictionary_id = dictionary_id;
	} else if (dict_size > dict_state.capacity) {
		throw InternalException("AggregateHT - using cached dictionary data but dictionary has changed (dictionary id "
		                        "%s - dict size %d, current capacity %d)",
		                        dict_state.dictionary_id, dict_size, dict_state.capacity);
	}

	auto &found_entry = dict_state.found_entry;
	auto &unique_entries = dict_state.unique_entries;
	idx_t unique_count = 0;
	// for each of the dictionary entries - check if we have already done a look-up into the hash table
	// if we have, we can just use the cached group pointers
	for (idx_t i = 0; i < groups.size(); i++) {
		auto dict_idx = offsets.get_index(i);
		unique_entries.set_index(unique_count, dict_idx);
		unique_count += !found_entry[dict_idx];
		found_entry[dict_idx] = true;
	}
	auto &new_dictionary_pointers = dict_state.new_dictionary_pointers;
	idx_t new_group_count = 0;
	if (unique_count > 0) {
		auto &unique_values = dict_state.unique_values;
		if (unique_values.ColumnCount() == 0) {
			unique_values.InitializeEmpty(groups.GetTypes());
		}
		// slice the dictionary
		unique_values.data[0].Slice(dictionary_vector, unique_entries, unique_count);
		unique_values.SetCardinality(unique_count);
		// now we know which entries we are going to add - hash them
		auto &hashes = dict_state.hashes;
		unique_values.Hash(hashes);

		// add the dictionary groups to the hash table
		new_group_count = FindOrCreateGroups(unique_values, hashes, new_dictionary_pointers, state.new_groups);
	}
	auto &aggregates = layout.GetAggregates();
	if (aggregates.empty()) {
		// early-out - no aggregates to update
		return new_group_count;
	}

	// set the addresses that we found for each of the unique groups in the main addresses vector
	auto new_dict_addresses = FlatVector::GetData<uintptr_t>(new_dictionary_pointers);
	// for each of the new groups, add them to the global (cached) list of addresses for the dictionary
	auto &dictionary_addresses = *dict_state.dictionary_addresses;
	auto dict_addresses = FlatVector::GetData<uintptr_t>(dictionary_addresses);
	for (idx_t i = 0; i < unique_count; i++) {
		auto dict_idx = unique_entries.get_index(i);
		dict_addresses[dict_idx] = new_dict_addresses[i] + layout.GetAggrOffset();
	}
	// now set up the addresses for the aggregates
	auto result_addresses = FlatVector::GetData<uintptr_t>(state.addresses);
	for (idx_t i = 0; i < groups.size(); i++) {
		auto dict_idx = offsets.get_index(i);
		result_addresses[i] = dict_addresses[dict_idx];
	}

	// finally process the aggregates
	UpdateAggregates(payload, filter);

	return new_group_count;
}

optional_idx GroupedAggregateHashTable::TryAddConstantGroups(DataChunk &groups, DataChunk &payload,
                                                             const unsafe_vector<idx_t> &filter) {
#ifndef DEBUG
	if (groups.size() <= 1) {
		// this only has a point if we have multiple groups
		return optional_idx();
	}
#endif
	auto &dict_state = state.dict_state;
	auto &unique_values = dict_state.unique_values;
	if (unique_values.ColumnCount() == 0) {
		unique_values.InitializeEmpty(groups.GetTypes());
	}
	// slice the dictionary
	unique_values.Reference(groups);
	unique_values.SetCardinality(1);
	unique_values.Flatten();

	auto &hashes = dict_state.hashes;
	unique_values.Hash(hashes);

	// add the single constant group to the hash table
	auto &new_dictionary_pointers = dict_state.new_dictionary_pointers;
	auto new_group_count = FindOrCreateGroups(unique_values, hashes, new_dictionary_pointers, state.new_groups);

	auto &aggregates = layout.GetAggregates();
	if (aggregates.empty()) {
		// early-out - no aggregates to update
		return new_group_count;
	}

	auto new_dict_addresses = FlatVector::GetData<uintptr_t>(new_dictionary_pointers);
	auto result_addresses = FlatVector::GetData<uintptr_t>(state.addresses);
	uintptr_t aggregate_address = new_dict_addresses[0] + layout.GetAggrOffset();
	for (idx_t i = 0; i < payload.size(); i++) {
		result_addresses[i] = aggregate_address;
	}

	// process the aggregates
	// FIXME: we can use simple_update here if the aggregates support it
	UpdateAggregates(payload, filter);

	return new_group_count;
}

optional_idx GroupedAggregateHashTable::TryAddCompressedGroups(DataChunk &groups, DataChunk &payload,
                                                               const unsafe_vector<idx_t> &filter) {
	// all groups must be compressed
	if (groups.AllConstant()) {
		return TryAddConstantGroups(groups, payload, filter);
	}
	if (groups.ColumnCount() == 1 && groups.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		return TryAddDictionaryGroups(groups, payload, filter);
	}
	return optional_idx();
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload, const unsafe_vector<idx_t> &filter) {
	sink_count += groups.size();

	// check if we can use an optimized path that utilizes compressed vectors
	auto result = TryAddCompressedGroups(groups, payload, filter);
	if (result.IsValid()) {
		return result.GetIndex();
	}
	// otherwise append the raw values
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);

	return AddChunk(groups, hashes, payload, filter);
}

void GroupedAggregateHashTable::UpdateAggregates(DataChunk &payload, const unsafe_vector<idx_t> &filter) {
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

	UpdateAggregates(payload, filter);

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
	const auto chunk_size = groups.size();
	if (Count() + chunk_size > capacity || Count() + chunk_size > ResizeThreshold()) {
		Verify();
		Resize(capacity * 2);
	}
	D_ASSERT(capacity - Count() >= chunk_size); // we need to be able to fit at least one vector of data

	// we start out with all entries [0, 1, 2, ..., chunk_size]
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
	TupleDataCollection::ToUnifiedFormat(state.partitioned_append_state.chunk_state, state.group_chunk);
	if (!state.group_data) {
		state.group_data = make_unsafe_uniq_array_uninitialized<UnifiedVectorFormat>(state.group_chunk.ColumnCount());
	}
	TupleDataCollection::GetVectorData(state.partitioned_append_state.chunk_state, state.group_data.get());

	group_hashes_v.Flatten(chunk_size);
	const auto hashes = FlatVector::GetData<hash_t>(group_hashes_v);

	addresses_v.Flatten(chunk_size);
	const auto addresses = FlatVector::GetData<data_ptr_t>(addresses_v);

	if (skip_lookups) {
		// Just appending now
		partitioned_data->AppendUnified(state.partitioned_append_state, state.group_chunk,
		                                *FlatVector::IncrementalSelectionVector(), chunk_size);
		RowOperations::InitializeStates(layout, state.partitioned_append_state.chunk_state.row_locations,
		                                *FlatVector::IncrementalSelectionVector(), chunk_size);

		const auto row_locations =
		    FlatVector::GetData<data_ptr_t>(state.partitioned_append_state.chunk_state.row_locations);
		const auto &row_sel = state.partitioned_append_state.reverse_partition_sel;
		for (idx_t i = 0; i < chunk_size; i++) {
			const auto &row_idx = row_sel[i];
			const auto &row_location = row_locations[row_idx];
			addresses[i] = row_location;
		}
		count += chunk_size;
		return chunk_size;
	}

	// Compute the entry in the table based on the hash using a modulo,
	// and precompute the hash salts for faster comparison below
	const auto ht_offsets = FlatVector::GetData<uint64_t>(state.ht_offsets);
	const auto hash_salts = FlatVector::GetData<hash_t>(state.hash_salts);

	// We also compute the occupied count, which is essentially useless.
	// However, this loop is branchless, while the main lookup loop below is not.
	// So, by doing the lookups here, we better amortize cache misses.
	idx_t occupied_count = 0;
	for (idx_t r = 0; r < chunk_size; r++) {
		const auto &hash = hashes[r];
		auto &ht_offset = ht_offsets[r];
		ht_offset = ApplyBitMask(hash);
		occupied_count += entries[ht_offset].IsOccupied(); // Lookup
		D_ASSERT(ht_offset == hash % capacity);
		hash_salts[r] = ht_entry_t::ExtractSalt(hash);
	}

	idx_t new_group_count = 0;
	idx_t remaining_entries = chunk_size;
	idx_t iteration_count;
	for (iteration_count = 0; remaining_entries > 0 && iteration_count < capacity; iteration_count++) {
		idx_t new_entry_count = 0;
		idx_t need_compare_count = 0;
		idx_t no_match_count = 0;

		// For each remaining entry, figure out whether or not it belongs to a full or empty group
		for (idx_t i = 0; i < remaining_entries; i++) {
			const auto index = sel_vector->get_index(i);
			const auto salt = hash_salts[index];
			auto &ht_offset = ht_offsets[index];

			idx_t inner_iteration_count;
			for (inner_iteration_count = 0; inner_iteration_count < capacity; inner_iteration_count++) {
				auto &entry = entries[ht_offset];
				if (!entry.IsOccupied()) { // Unoccupied: claim it
					entry.SetSalt(salt);
					state.empty_vector.set_index(new_entry_count++, index);
					new_groups_out.set_index(new_group_count++, index);
					break;
				}

				if (DUCKDB_LIKELY(entry.GetSalt() == salt)) { // Matching salt: compare groups
					state.group_compare_vector.set_index(need_compare_count++, index);
					break;
				}

				// Linear probing
				IncrementAndWrap(ht_offset, bitmask);
			}
			if (DUCKDB_UNLIKELY(inner_iteration_count == capacity)) {
				throw InternalException("Maximum inner iteration count reached in GroupedAggregateHashTable");
			}
		}

		if (DUCKDB_UNLIKELY(occupied_count > new_entry_count + need_compare_count)) {
			// We use the useless occupied_count we summed above here so the variable is used,
			// and the compiler cannot optimize away the vectorized lookups above. This should never be triggered.
			throw InternalException("Internal validation failed in GroupedAggregateHashTable");
		}
		occupied_count = 0; // Have to set to 0 for next iterations

		if (new_entry_count != 0) {
			// Append everything that belongs to an empty group
			optional_ptr<PartitionedTupleData> data;
			optional_ptr<PartitionedTupleDataAppendState> append_state;
			if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD &&
			    new_entry_count / RadixPartitioning::NumberOfPartitions(radix_bits) <= 4) {
				TupleDataCollection::ToUnifiedFormat(state.unpartitioned_append_state.chunk_state, state.group_chunk);
				data = unpartitioned_data.get();
				append_state = &state.unpartitioned_append_state;
			} else {
				data = partitioned_data.get();
				append_state = &state.partitioned_append_state;
			}
			data->AppendUnified(*append_state, state.group_chunk, state.empty_vector, new_entry_count);
			RowOperations::InitializeStates(layout, append_state->chunk_state.row_locations,
			                                *FlatVector::IncrementalSelectionVector(), new_entry_count);

			// Set the entry pointers in the 1st part of the HT now that the data has been appended
			const auto row_locations = FlatVector::GetData<data_ptr_t>(append_state->chunk_state.row_locations);
			const auto &row_sel = append_state->reverse_partition_sel;
			for (idx_t new_entry_idx = 0; new_entry_idx < new_entry_count; new_entry_idx++) {
				const auto &index = state.empty_vector[new_entry_idx];
				const auto &row_idx = row_sel[index];
				const auto &row_location = row_locations[row_idx];

				auto &entry = entries[ht_offsets[index]];

				entry.SetPointer(row_location);
				addresses[index] = row_location;
			}
		}

		if (need_compare_count != 0) {
			// Get the pointers to the rows that need to be compared
			for (idx_t need_compare_idx = 0; need_compare_idx < need_compare_count; need_compare_idx++) {
				const auto &index = state.group_compare_vector[need_compare_idx];
				const auto &entry = entries[ht_offsets[index]];
				addresses[index] = entry.GetPointer();
			}

			// Perform group comparisons
			row_matcher.Match(state.group_chunk, state.partitioned_append_state.chunk_state.vector_data,
			                  state.group_compare_vector, need_compare_count, layout, addresses_v,
			                  &state.no_match_vector, no_match_count);
		}

		// Linear probing: each of the entries that do not match move to the next entry in the HT
		for (idx_t i = 0; i < no_match_count; i++) {
			const auto &index = state.no_match_vector[i];
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
	auto other_partitioned_data = other.AcquirePartitionedData();
	auto other_data = other_partitioned_data->GetUnpartitioned();
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
		// Check for interrupts with each chunk
		if (context.interrupted) {
			throw InterruptException();
		}
		const auto input_chunk_size = fm_state.groups.size();
		FindOrCreateGroups(fm_state.groups, fm_state.hashes, fm_state.group_addresses, fm_state.new_groups_sel);
		RowOperations::CombineStates(row_state, layout, fm_state.scan_state.chunk_state.row_locations,
		                             fm_state.group_addresses, input_chunk_size);
		if (layout.HasDestructor()) {
			RowOperations::DestroyStates(row_state, layout, fm_state.scan_state.chunk_state.row_locations,
			                             input_chunk_size);
		}

		if (progress) {
			*progress = static_cast<double>(++chunk_idx) / static_cast<double>(chunk_count);
		}
	}

	Verify();
}

void GroupedAggregateHashTable::InitializeScan(AggregateHTScanState &scan_state) {
	scan_state.partition_idx = 0;
	vector<idx_t> group_indexes(layout.ColumnCount() - 1);
	for (idx_t i = 0; i < group_indexes.size(); i++) {
		group_indexes[i] = i;
	}

	auto &partition = partitioned_data->GetPartitions()[scan_state.partition_idx];
	partition->InitializeScan(scan_state.scan_states, group_indexes);
}

bool GroupedAggregateHashTable::Scan(AggregateHTScanState &scan_state, DataChunk &distinct_rows,
                                     DataChunk &payload_rows) {
	if (scan_state.partition_idx >= partitioned_data->PartitionCount()) {
		return false;
	}

	payload_rows.Reset();
	distinct_rows.Reset();
	auto &current_partition = partitioned_data->GetPartitions()[scan_state.partition_idx];

	if (current_partition->Scan(scan_state.scan_states, distinct_rows)) {
		FetchAggregates(distinct_rows, payload_rows);
		return true;
	} else {
		if (++(scan_state.partition_idx) >= partitioned_data->PartitionCount()) {
			return false;
		} else {
			auto &new_partition = partitioned_data->GetPartitions()[scan_state.partition_idx];
			new_partition->InitializeScan(scan_state.scan_states);
			return true;
		}
	}
}
} // namespace duckdb
