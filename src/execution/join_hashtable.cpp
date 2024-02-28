#include "duckdb/execution/join_hashtable.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {
using ValidityBytes = JoinHashTable::ValidityBytes;
using ScanStructure = JoinHashTable::ScanStructure;
using ProbeSpill = JoinHashTable::ProbeSpill;
using ProbeSpillLocalState = JoinHashTable::ProbeSpillLocalAppendState;

JoinHashTable::ProbeState::ProbeState()
    : hash_salts_v(LogicalType::UBIGINT), ht_offsets_v(LogicalType::UBIGINT), row_ptr_insert_to_v(LogicalType::POINTER),
      key_no_match_sel(STANDARD_VECTOR_SIZE), salt_match_sel(STANDARD_VECTOR_SIZE) {
}

JoinHashTable::InsertState::InsertState(unique_ptr<TupleDataCollection> &data_collection) {
	data_collection->InitializeChunk(lhs_data);
	data_collection->InitializeChunkState(chunk_state);
}
JoinHashTable::JoinHashTable(BufferManager &buffer_manager_p, const vector<JoinCondition> &conditions_p,
                             vector<LogicalType> btypes, JoinType type_p, const vector<idx_t> &output_columns_p)
    : buffer_manager(buffer_manager_p), conditions(conditions_p), build_types(std::move(btypes)),
      output_columns(output_columns_p), entry_size(0), tuple_size(0), vfound(Value::BOOLEAN(false)), join_type(type_p),
      finalized(false), has_null(false), radix_bits(INITIAL_RADIX_BITS), partition_start(0), partition_end(0) {

	for (idx_t i = 0; i < conditions.size(); ++i) {
		auto &condition = conditions[i];
		D_ASSERT(condition.left->return_type == condition.right->return_type);
		auto type = condition.left->return_type;
		if (condition.comparison == ExpressionType::COMPARE_EQUAL ||
		    condition.comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {

			// ensure that all equality conditions are at the front,
			// and that all other conditions are at the back
			D_ASSERT(equality_types.size() == condition_types.size());
			equality_types.push_back(type);
			equality_predicates.push_back(condition.comparison);

		} else {
			// all non-equality conditions are at the back
			non_equality_predicates.push_back(condition.comparison);
			non_equality_predicate_columns.push_back(i);
		}

		null_values_are_equal.push_back(condition.comparison == ExpressionType::COMPARE_DISTINCT_FROM ||
		                                condition.comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM);

		condition_types.push_back(type);
	}
	// at least one equality is necessary
	D_ASSERT(!equality_types.empty());

	// Types for the layout
	vector<LogicalType> layout_types(condition_types);
	layout_types.insert(layout_types.end(), build_types.begin(), build_types.end());
	if (PropagatesBuildSide(join_type)) {
		// full/right outer joins need an extra bool to keep track of whether or not a tuple has found a matching entry
		// we place the bool before the NEXT pointer
		layout_types.emplace_back(LogicalType::BOOLEAN);
	}
	layout_types.emplace_back(LogicalType::HASH);
	layout.Initialize(layout_types, false);

	// Initialize the row matcher that are used for filtering during the probing only if there are non-equality
	if (!non_equality_predicates.empty()) {

		row_matcher_probe = unique_ptr<RowMatcher>(new RowMatcher());
		row_matcher_probe_no_match_sel = unique_ptr<RowMatcher>(new RowMatcher());

		row_matcher_probe->Initialize(false, layout, non_equality_predicates, non_equality_predicate_columns);
		row_matcher_probe_no_match_sel->Initialize(true, layout, non_equality_predicates,
		                                           non_equality_predicate_columns);
	}

	// todo: What happens if there is a predicate that is dependent on the probe side? Need to filter this out
	row_matcher_build.Initialize(true, layout, equality_predicates);

	const auto &offsets = layout.GetOffsets();
	tuple_size = offsets[condition_types.size() + build_types.size()];
	pointer_offset = offsets.back();
	entry_size = layout.GetRowWidth();

	data_collection = make_uniq<TupleDataCollection>(buffer_manager, layout);
	sink_collection =
	    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, radix_bits, layout.ColumnCount() - 1);
}

JoinHashTable::~JoinHashTable() {
}

void JoinHashTable::Merge(JoinHashTable &other) {
	{
		lock_guard<mutex> guard(data_lock);
		data_collection->Combine(*other.data_collection);
	}

	if (join_type == JoinType::MARK) {
		auto &info = correlated_mark_join_info;
		lock_guard<mutex> mj_lock(info.mj_lock);
		has_null = has_null || other.has_null;
		if (!info.correlated_types.empty()) {
			auto &other_info = other.correlated_mark_join_info;
			info.correlated_counts->Combine(*other_info.correlated_counts);
		}
	}

	sink_collection->Combine(*other.sink_collection);
}

static void ApplyBitmaskAndGetSalt(Vector &hashes_v, const idx_t &count, const idx_t &bitmask, Vector &ht_offsets_v,
                                   Vector &hash_salts_v, const SelectionVector &sel) {

	auto hash_salts = FlatVector::GetData<idx_t>(hash_salts_v);
	auto ht_offsets = FlatVector::GetData<idx_t>(ht_offsets_v);

	if (hashes_v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		D_ASSERT(!ConstantVector::IsNull(hashes_v));
		auto indices = ConstantVector::GetData<hash_t>(hashes_v);
		hash_t salt = aggr_ht_entry_t::ExtractSalt(*indices);
		idx_t offset = *indices & bitmask;
		hashes_v.Flatten(count);

		for (idx_t i = 0; i < count; i++) {
			hash_salts[i] = salt;
			ht_offsets[i] = offset;
		}
	} else {
		UnifiedVectorFormat hashes_v_unified;
		hashes_v.ToUnifiedFormat(count, hashes_v_unified);

		auto hash_data = UnifiedVectorFormat::GetData<hash_t>(hashes_v_unified);

		for (idx_t i = 0; i < count; i++) {
			auto row_index = sel.get_index(i);
			auto hash_index = hashes_v_unified.sel->get_index(row_index);
			auto hash = hash_data[hash_index];

			auto ht_offset = hash & bitmask;
			ht_offsets[row_index] = ht_offset;
			hash_salts[row_index] = aggr_ht_entry_t::ExtractSalt(hash);
		}
	}
}


// uses an AND operation to apply the bitmask instead of an in condition
inline void IncrementAndWrap(idx_t &value, idx_t increment, uint64_t bitmask) {
	value += increment;
	value &= bitmask;
}

inline void IncrementAndWrap(idx_t &value, uint64_t bitmask) {
	IncrementAndWrap(value, 1, bitmask);
}

static void AppendSelectionVector(SelectionVector &sel, const idx_t &from, const SelectionVector &other,
                                  const idx_t &count) {
	for (idx_t i = 0; i < count; i++) {
		sel.set_index(from + i, other.get_index(i));
	}
}

void JoinHashTable::GetRowPointers(DataChunk &keys, TupleDataChunkState &key_state, ProbeState &state, Vector &hashes_v,
                                   const SelectionVector &sel, idx_t count, Vector &pointers) {

	ApplyBitmaskAndGetSalt(hashes_v, count, bitmask, state.ht_offsets_v, state.hash_salts_v, sel);

	// After teh ApplyBitmaskAndGetSalt the hashes_v will contain the exact offsets into the HT
	auto ht_offsets = FlatVector::GetData<idx_t>(state.ht_offsets_v);
	auto hash_salts = FlatVector::GetData<idx_t>(state.hash_salts_v);
	auto pointers_data = FlatVector::GetData<data_ptr_t>(pointers);
	auto row_ptr_insert_to = FlatVector::GetData<data_ptr_t>(state.row_ptr_insert_to_v);

	const SelectionVector *remaining_sel = &sel;
	idx_t remaining_count = count;

	// untill every entry has been processed
	while (remaining_count > 0) {

		idx_t salt_match_count = 0;
		idx_t key_no_match_count = 0;

		// for each entry, linear probing until
		// a) an empty entry is found -> return nullptr (do nothing, as vector is zeroed)
		// b) an entry is found where the salt matches -> need to compare the keys
		for (idx_t i = 0; i < count; i++) {
			const auto row_index = remaining_sel->get_index(i);
			auto &ht_offset = ht_offsets[row_index];

			idx_t increment;

			// increment the ht_offset of the entry as long as next entry is occupied and salt does not match
			do {

				auto &entry = entries[ht_offset];
				bool occupied = entry.IsOccupied();
				bool salt_match = entry.GetSalt() == hash_salts[row_index];

				// the entries we need to process in the next iteration are the ones that are occupied and the salt
				// does not match, the ones that are empty need no further processing
				bool occupied_and_salt_match = occupied && salt_match;
				state.salt_match_sel.set_index(salt_match_count, row_index);
				salt_match_count += occupied_and_salt_match;

				// condition for incrementing the ht_offset: occupied and salt does not match -> move to next entry
				increment = occupied && !salt_match;
				IncrementAndWrap(ht_offset, increment, bitmask);

			} while (increment);
		}

		// at this step, for all the pointers we stepped either until we found an empty entry or an entry with a
		// matching salt, we need to compare the keys for the ones that have a matching salt

		if (salt_match_count > 0) {
			// Get the pointers to the rows that need to be compared
			for (idx_t need_compare_idx = 0; need_compare_idx < salt_match_count; need_compare_idx++) {
				const auto row_index = state.salt_match_sel.get_index(need_compare_idx);
				const auto &entry = entries[ht_offsets[row_index]];
				row_ptr_insert_to[row_index] = entry.GetPointer();
			}

			// Perform row comparisons, after function call salt_match_sel will point to the keys that match

			idx_t key_match_count =
				row_matcher_build.Match(keys, key_state.vector_data, state.salt_match_sel, salt_match_count, layout,
										state.row_ptr_insert_to_v, &state.key_no_match_sel, key_no_match_count);

			D_ASSERT(key_match_count + key_no_match_count == salt_match_count);

			// Set a pointer to the matching row
			for (idx_t i = 0; i < key_match_count; i++) {
				const auto row_index = state.salt_match_sel.get_index(i);
				auto &entry = entries[ht_offsets[row_index]];
				pointers_data[row_index] = entry.GetPointer();
			}

			// update the ht_offset to point to the next entry for the ones that did not match
			for (idx_t i = 0; i < key_no_match_count; i++) {
				const auto row_index = state.key_no_match_sel.get_index(i);
				auto &ht_offset = ht_offsets[row_index];

				IncrementAndWrap(ht_offset, 1, bitmask);
			}

		}

		remaining_sel = &state.key_no_match_sel;
		remaining_count = key_no_match_count;

	}
}

void JoinHashTable::Hash(DataChunk &keys, const SelectionVector &sel, idx_t count, Vector &hashes) {
	if (count == keys.size()) {
		// no null values are filtered: use regular hash functions
		VectorOperations::Hash(keys.data[0], hashes, keys.size());
		for (idx_t i = 1; i < equality_types.size(); i++) {
			VectorOperations::CombineHash(hashes, keys.data[i], keys.size());
		}
	} else {
		// null values were filtered: use selection vector
		VectorOperations::Hash(keys.data[0], hashes, sel, count);
		for (idx_t i = 1; i < equality_types.size(); i++) {
			VectorOperations::CombineHash(hashes, keys.data[i], sel, count);
		}
	}
}

static idx_t FilterNullValues(UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t count,
                              SelectionVector &result) {
	idx_t result_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto key_idx = vdata.sel->get_index(idx);
		if (vdata.validity.RowIsValid(key_idx)) {
			result.set_index(result_count++, idx);
		}
	}
	return result_count;
}

void JoinHashTable::Build(PartitionedTupleDataAppendState &append_state, DataChunk &keys, DataChunk &payload) {
	D_ASSERT(!finalized);
	D_ASSERT(keys.size() == payload.size());
	if (keys.size() == 0) {
		return;
	}
	// special case: correlated mark join
	if (join_type == JoinType::MARK && !correlated_mark_join_info.correlated_types.empty()) {
		auto &info = correlated_mark_join_info;
		lock_guard<mutex> mj_lock(info.mj_lock);
		// Correlated MARK join
		// for the correlated mark join we need to keep track of COUNT(*) and COUNT(COLUMN) for each of the correlated
		// columns push into the aggregate hash table
		D_ASSERT(info.correlated_counts);
		info.group_chunk.SetCardinality(keys);
		for (idx_t i = 0; i < info.correlated_types.size(); i++) {
			info.group_chunk.data[i].Reference(keys.data[i]);
		}
		if (info.correlated_payload.data.empty()) {
			vector<LogicalType> types;
			types.push_back(keys.data[info.correlated_types.size()].GetType());
			info.correlated_payload.InitializeEmpty(types);
		}
		info.correlated_payload.SetCardinality(keys);
		info.correlated_payload.data[0].Reference(keys.data[info.correlated_types.size()]);
		info.correlated_counts->AddChunk(info.group_chunk, info.correlated_payload, AggregateType::NON_DISTINCT);
	}

	// build a chunk to append to the data collection [keys, payload, (optional "found" boolean), hash]
	DataChunk source_chunk;
	source_chunk.InitializeEmpty(layout.GetTypes());
	for (idx_t i = 0; i < keys.ColumnCount(); i++) {
		source_chunk.data[i].Reference(keys.data[i]);
	}
	idx_t col_offset = keys.ColumnCount();
	D_ASSERT(build_types.size() == payload.ColumnCount());
	for (idx_t i = 0; i < payload.ColumnCount(); i++) {
		source_chunk.data[col_offset + i].Reference(payload.data[i]);
	}
	col_offset += payload.ColumnCount();
	if (PropagatesBuildSide(join_type)) {
		// for FULL/RIGHT OUTER joins initialize the "found" boolean to false
		source_chunk.data[col_offset].Reference(vfound);
		col_offset++;
	}
	Vector hash_values(LogicalType::HASH);
	source_chunk.data[col_offset].Reference(hash_values);
	source_chunk.SetCardinality(keys);

	// ToUnifiedFormat the source chunk
	TupleDataCollection::ToUnifiedFormat(append_state.chunk_state, source_chunk);

	// prepare the keys for processing
	const SelectionVector *current_sel;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t added_count = PrepareKeys(keys, append_state.chunk_state.vector_data, current_sel, sel, true);
	if (added_count < keys.size()) {
		has_null = true;
	}
	if (added_count == 0) {
		return;
	}

	// hash the keys and obtain an entry in the list
	// note that we only hash the keys used in the equality comparison
	Hash(keys, *current_sel, added_count, hash_values);

	// Re-reference and ToUnifiedFormat the hash column after computing it
	source_chunk.data[col_offset].Reference(hash_values);
	hash_values.ToUnifiedFormat(source_chunk.size(), append_state.chunk_state.vector_data.back().unified);

	// We already called TupleDataCollection::ToUnifiedFormat, so we can AppendUnified here
	sink_collection->AppendUnified(append_state, source_chunk, *current_sel, added_count);
}

idx_t JoinHashTable::PrepareKeys(DataChunk &keys, vector<TupleDataVectorFormat> &vector_data,
                                 const SelectionVector *&current_sel, SelectionVector &sel, bool build_side) {
	// figure out which keys are NULL, and create a selection vector out of them
	current_sel = FlatVector::IncrementalSelectionVector();
	idx_t added_count = keys.size();
	if (build_side && (PropagatesBuildSide(join_type))) {
		// in case of a right or full outer join, we cannot remove NULL keys from the build side
		return added_count;
	}

	for (idx_t col_idx = 0; col_idx < keys.ColumnCount(); col_idx++) {
		if (!null_values_are_equal[col_idx]) {
			auto &col_key_data = vector_data[col_idx].unified;
			if (col_key_data.validity.AllValid()) {
				continue;
			}
			added_count = FilterNullValues(col_key_data, *current_sel, added_count, sel);
			// null values are NOT equal for this column, filter them out
			current_sel = &sel;
		}
	}
	return added_count;
}

template <bool PARALLEL>
static inline void InsertRowToEntry(atomic<aggr_ht_entry_t> &entry, data_ptr_t row_ptr_to_insert, const hash_t salt,
                                    const idx_t pointer_offset) {

	if (PARALLEL) {
		aggr_ht_entry_t current_entry = entry.load();
		aggr_ht_entry_t new_entry;
		do {
			data_ptr_t current_row_pointer = current_entry.GetPointerOrNull();
			Store<data_ptr_t>(current_row_pointer, row_ptr_to_insert + pointer_offset);
			new_entry = aggr_ht_entry_t::GetDesiredEntry(row_ptr_to_insert, salt);
		} while (!std::atomic_compare_exchange_weak(&entry, &current_entry, new_entry));

	} else {
		aggr_ht_entry_t current_entry = entry.load();
		data_ptr_t current_row_pointer = current_entry.GetPointerOrNull();
		Store<data_ptr_t>(current_row_pointer, row_ptr_to_insert + pointer_offset);

		entry = aggr_ht_entry_t::GetDesiredEntry(row_ptr_to_insert, salt);
	}
}

template <bool PARALLEL>
static void InsertHashesLoop(atomic<aggr_ht_entry_t> entries[], Vector row_locations, Vector &hashes_v, idx_t count,
                             JoinHashTable::InsertState &state, const idx_t pointer_offset, const idx_t capacity,
                             unique_ptr<TupleDataCollection> &data_collection, RowMatcher &row_matcher_build,
                             const TupleDataLayout &layout, const idx_t bitmask) {

	D_ASSERT(hashes_v.GetType().id() == LogicalType::HASH);

	// we start out with all entries [0, 1, 2, ..., groups.size()]
	const SelectionVector *sel_vector = FlatVector::IncrementalSelectionVector();

	ApplyBitmaskAndGetSalt(hashes_v, count, bitmask, state.ht_offsets_v, state.hash_salts_v, *sel_vector);

	// the offset for each row to insert
	auto ht_offsets = FlatVector::GetData<idx_t>(state.ht_offsets_v);
	auto hash_salts = FlatVector::GetData<idx_t>(state.hash_salts_v);
	auto row_ptr_insert_to = FlatVector::GetData<data_ptr_t>(state.row_ptr_insert_to_v);
	auto row_ptrs_to_insert = FlatVector::GetData<data_ptr_t>(row_locations);

	idx_t remaining_entries = count;

	// This vector will select all rows we need to compare
	SelectionVector entry_compare_sel_vector(STANDARD_VECTOR_SIZE);

	// Will hold all rows that where compared and did not match
	SelectionVector no_match_sel(STANDARD_VECTOR_SIZE);

	while (remaining_entries > 0) {

		idx_t need_compare_count = 0;
		idx_t no_match_count = 0;

		// iterate over each entry to find out whether it belongs to an existing list or will start
		// a new list
		for (idx_t i = 0; i < remaining_entries; i++) {
			const auto entry_index = sel_vector->get_index(i);
			auto &ht_offset = ht_offsets[entry_index];
			auto salt = hash_salts[entry_index];

			// This loop either stops if an empty entry is found or if the value to insert needs to be compared
			// with a row for potential insertion
			while (true) {
				atomic<aggr_ht_entry_t> &entry = entries[ht_offset];

				if (entry.load().IsOccupied()) {

					if (entry.load().GetSalt() == salt) {
						// Same salt, compare group keys
						entry_compare_sel_vector.set_index(need_compare_count, entry_index);
						need_compare_count++;
						break;
					} else {
						// Different salts, move to next entry (linear probing)
						ht_offset++;
						if (ht_offset >= capacity) {
							ht_offset = 0;
						}
						continue;
					}
				} else { // entry is not occupied, so let's claim it and start a new list

					// Still need to set nullptr at the end to mark the end of the list
					data_ptr_t row_ptr = row_ptrs_to_insert[entry_index];
					InsertRowToEntry<PARALLEL>(entry, row_ptr, salt, pointer_offset);
					break;
				}
			}
		}

		if (need_compare_count != 0) {

			// makes sure DataChunk has the right size, we will use the same index space for the chunk as for the
			// overall hashes_v so make sure they have the same size. The DataChunk will however only be sparsely
			// filled based on the entries that need a comparison
			state.lhs_data.SetCardinality(count);

			// The target selection vector says where to write the results into the lhs_data, we just want to write
			// sequentially
			// We can't use the standard flat vector as this one has no internal selection vector and the Match
			// Function Also marks the found rows in the selection vector, so we need to make sure there is one
			data_collection->Gather(row_locations, entry_compare_sel_vector, need_compare_count, state.lhs_data,
			                        entry_compare_sel_vector, state.chunk_state.cached_cast_vectors);

			TupleDataCollection::ToUnifiedFormat(state.chunk_state, state.lhs_data);

			vector<TupleDataVectorFormat> &lhs_formats = state.chunk_state.vector_data;

			// Get the pointers to the rows that need to be compared
			for (idx_t need_compare_idx = 0; need_compare_idx < need_compare_count; need_compare_idx++) {
				const auto entry_index = entry_compare_sel_vector.get_index(need_compare_idx);
				const auto &entry = entries[ht_offsets[entry_index]];
				row_ptr_insert_to[entry_index] = entry.load().GetPointer();
			}

			// Perform row comparisons
			idx_t match_count =
			    row_matcher_build.Match(state.lhs_data, lhs_formats, entry_compare_sel_vector, need_compare_count,
			                            layout, state.row_ptr_insert_to_v, &no_match_sel, no_match_count);

			D_ASSERT(match_count + no_match_count == need_compare_count);
			// Insert the rows that match
			for (idx_t i = 0; i < match_count; i++) {
				const auto entry_index = entry_compare_sel_vector.get_index(i);

				auto &entry = entries[ht_offsets[entry_index]];
				data_ptr_t row_ptr = row_ptrs_to_insert[entry_index];
				auto salt = hash_salts[entry_index];
				InsertRowToEntry<PARALLEL>(entry, row_ptr, salt, pointer_offset);
			}
		}

		// Linear probing: each of the entries that do not match move to the next entry in the HT
		for (idx_t i = 0; i < no_match_count; i++) {
			const auto entry_index = no_match_sel.get_index(i);
			auto &ht_offset = ht_offsets[entry_index];

			ht_offset++;
			if (ht_offset >= capacity) {
				ht_offset = 0;
			}
		}

		// update the overall selection vector to only point the entries that still need to be inserted
		// as there was no match found for them yet
		sel_vector = &no_match_sel;
		remaining_entries = no_match_count;
	}
}

void JoinHashTable::InsertHashes(Vector &hashes_v, idx_t count, TupleDataChunkState &chunk_state,
                                 InsertState &insert_state, bool parallel) {
	auto atomic_entries = reinterpret_cast<atomic<aggr_ht_entry_t> *>(this->entries);
	auto row_locations = chunk_state.row_locations;

	if (parallel) {
		InsertHashesLoop<true>(atomic_entries, row_locations, hashes_v, count, insert_state, pointer_offset, capacity,
		                       data_collection, row_matcher_build, layout, bitmask);
	} else {
		InsertHashesLoop<false>(atomic_entries, row_locations, hashes_v, count, insert_state, pointer_offset, capacity,
		                        data_collection, row_matcher_build, layout, bitmask);
	}
}

void JoinHashTable::InitializePointerTable() {
	capacity = PointerTableCapacity(Count());
	D_ASSERT(IsPowerOfTwo(capacity));

	if (hash_map.get()) {
		// There is already a hash map
		auto current_capacity = hash_map.GetSize() / sizeof(aggr_ht_entry_t);
		if (capacity != current_capacity) {
			// Different size, re-allocate
			hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(aggr_ht_entry_t));
			entries = reinterpret_cast<aggr_ht_entry_t *>(hash_map.get());
		}
	} else {
		// Allocate a hash map
		hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(aggr_ht_entry_t));
		entries = reinterpret_cast<aggr_ht_entry_t *>(hash_map.get());
	}
	D_ASSERT(hash_map.GetSize() == capacity * sizeof(aggr_ht_entry_t));

	// initialize HT with all-zero entries
	std::fill_n(entries, capacity, aggr_ht_entry_t(0));

	bitmask = capacity - 1;
}

void JoinHashTable::Finalize(idx_t chunk_idx_from, idx_t chunk_idx_to, bool parallel) {

	// Pointer table should be allocated
	D_ASSERT(hash_map.get());

	Vector hashes(LogicalType::HASH);
	auto hash_data = FlatVector::GetData<hash_t>(hashes);

	TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::KEEP_EVERYTHING_PINNED, chunk_idx_from,
	                                chunk_idx_to, false);
	const auto row_locations = iterator.GetRowLocations();

	InsertState insert_state(data_collection);

	do {

		const auto count = iterator.GetCurrentChunkCount();
		for (idx_t i = 0; i < count; i++) {
			hash_data[i] = Load<hash_t>(row_locations[i] + pointer_offset);
		}
		TupleDataChunkState &chunkState = iterator.GetChunkState();

		InsertHashes(hashes, count, chunkState, insert_state, parallel);
	} while (iterator.Next());
}

unique_ptr<ScanStructure> JoinHashTable::InitializeScanStructure(DataChunk &keys, TupleDataChunkState &key_state,
                                                                 const SelectionVector *&current_sel) {
	D_ASSERT(Count() > 0); // should be handled before
	D_ASSERT(finalized);

	// set up the scan structure
	auto ss = make_uniq<ScanStructure>(*this, key_state);

	if (join_type != JoinType::INNER) {
		ss->found_match = make_unsafe_uniq_array<bool>(STANDARD_VECTOR_SIZE);
		memset(ss->found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
	}

	// first prepare the keys for probing
	TupleDataCollection::ToUnifiedFormat(key_state, keys);
	ss->count = PrepareKeys(keys, key_state.vector_data, current_sel, ss->sel_vector, false);
	return ss;
}

unique_ptr<ScanStructure> JoinHashTable::Probe(DataChunk &keys, TupleDataChunkState &key_state, ProbeState &probe_state,
                                               Vector *precomputed_hashes) {
	const SelectionVector *current_sel;
	auto ss = InitializeScanStructure(keys, key_state, current_sel);
	if (ss->count == 0) {
		return ss;
	}

	if (precomputed_hashes) {
		GetRowPointers(keys, key_state, probe_state, *precomputed_hashes, *current_sel, ss->count, ss->pointers);
	} else {
		// hash all the keys
		Vector hashes(LogicalType::HASH);
		Hash(keys, *current_sel, ss->count, hashes);

		// now initialize the pointers of the scan structure based on the hashes
		GetRowPointers(keys, key_state, probe_state, hashes, *current_sel, ss->count, ss->pointers);
	}

	// create the selection vector linking to only non-empty entries
	ss->InitializeSelectionVector(current_sel);

	return ss;
}

ScanStructure::ScanStructure(JoinHashTable &ht_p, TupleDataChunkState &key_state_p)
    : key_state(key_state_p), pointers(LogicalType::POINTER), sel_vector(STANDARD_VECTOR_SIZE), ht(ht_p),
      finished(false) {
}

void ScanStructure::Next(DataChunk &keys, DataChunk &left, DataChunk &result) {
	if (finished) {
		return;
	}
	switch (ht.join_type) {
	case JoinType::INNER:
	case JoinType::RIGHT:
	case JoinType::RIGHT_ANTI:
	case JoinType::RIGHT_SEMI:
		NextInnerJoin(keys, left, result);
		break;
	case JoinType::SEMI:
		NextSemiJoin(keys, left, result);
		break;
	case JoinType::MARK:
		NextMarkJoin(keys, left, result);
		break;
	case JoinType::ANTI:
		NextAntiJoin(keys, left, result);
		break;
	case JoinType::OUTER:
	case JoinType::LEFT:
		NextLeftJoin(keys, left, result);
		break;
	case JoinType::SINGLE:
		NextSingleJoin(keys, left, result);
		break;
	default:
		throw InternalException("Unhandled join type in JoinHashTable");
	}
}

bool ScanStructure::PointersExhausted() const {
	// AdvancePointers creates a "new_count" for every pointer advanced during the
	// previous advance pointers call. If no pointers are advanced, new_count = 0.
	// count is then set ot new_count.
	return count == 0;
}

idx_t ScanStructure::ResolvePredicates(DataChunk &keys, SelectionVector &match_sel, SelectionVector *no_match_sel) {
	// Start with the scan selection
	for (idx_t i = 0; i < this->count; ++i) {
		match_sel.set_index(i, this->sel_vector.get_index(i));
	}
	idx_t no_match_count = 0;

	auto &matcher = no_match_sel ? ht.row_matcher_probe_no_match_sel : ht.row_matcher_probe;
	// If there is a matcher for the probing side because of non-equality predicates, use it
	if (matcher) {

		// we need to only use the vectors with the indices of the columns that are used in the probe phase, namely
		// the non-equality columns

		return matcher->Match(keys, key_state.vector_data, match_sel, this->count, ht.layout, pointers, no_match_sel,
		                      no_match_count);
	} else { // Otherwise, just return the count as we assume that the HT chains are already filtered
		return this->count;
	}
}

idx_t ScanStructure::ScanInnerJoin(DataChunk &keys, SelectionVector &result_vector) {
	while (true) {
		// resolve the equality_predicates for this set of keys
		idx_t result_count = ResolvePredicates(keys, result_vector, nullptr);

		// after doing all the comparisons set the found_match vector
		if (found_match) {
			for (idx_t i = 0; i < result_count; i++) {
				auto idx = result_vector.get_index(i);
				found_match[idx] = true;
			}
		}
		if (result_count > 0) {
			return result_count;
		}
		// no matches found: check the next set of pointers
		AdvancePointers();
		if (this->count == 0) {
			return 0;
		}
	}
}

void ScanStructure::AdvancePointers(const SelectionVector &sel, idx_t sel_count) {
	// now for all the pointers, we move on to the next set of pointers
	idx_t new_count = 0;
	auto ptrs = FlatVector::GetData<data_ptr_t>(this->pointers);
	for (idx_t i = 0; i < sel_count; i++) {
		auto idx = sel.get_index(i);
		ptrs[idx] = Load<data_ptr_t>(ptrs[idx] + ht.pointer_offset);
		if (ptrs[idx]) {
			this->sel_vector.set_index(new_count++, idx);
		}
	}
	this->count = new_count;
}

void ScanStructure::InitializeSelectionVector(const SelectionVector *&current_sel) {
	idx_t non_empty_count = 0;
	auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);
	auto cnt = count;
	for (idx_t i = 0; i < cnt; i++) {
		const auto idx = current_sel->get_index(i);
		if (ptrs[idx]) {
			sel_vector.set_index(non_empty_count++, idx);
		}
	}
	count = non_empty_count;
}

void ScanStructure::AdvancePointers() {
	AdvancePointers(this->sel_vector, this->count);
}

void ScanStructure::GatherResult(Vector &result, const SelectionVector &result_vector,
                                 const SelectionVector &sel_vector, const idx_t count, const idx_t col_no) {
	ht.data_collection->Gather(pointers, sel_vector, count, col_no, result, result_vector, nullptr);
}

void ScanStructure::GatherResult(Vector &result, const SelectionVector &sel_vector, const idx_t count,
                                 const idx_t col_idx) {
	GatherResult(result, *FlatVector::IncrementalSelectionVector(), sel_vector, count, col_idx);
}

void ScanStructure::NextInnerJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	if (ht.join_type != JoinType::RIGHT_SEMI && ht.join_type != JoinType::RIGHT_ANTI) {
		D_ASSERT(result.ColumnCount() == left.ColumnCount() + ht.output_columns.size());
	}
	if (this->count == 0) {
		// no pointers left to chase
		return;
	}

	SelectionVector result_vector(STANDARD_VECTOR_SIZE);

	idx_t result_count = ScanInnerJoin(keys, result_vector);
	if (result_count > 0) {
		if (PropagatesBuildSide(ht.join_type)) {
			// full/right outer join: mark join matches as FOUND in the HT
			auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);
			for (idx_t i = 0; i < result_count; i++) {
				auto idx = result_vector.get_index(i);
				// NOTE: threadsan reports this as a data race because this can be set concurrently by separate
				// threads Technically it is, but it does not matter, since the only value that can be written is
				// "true"
				Store<bool>(true, ptrs[idx] + ht.tuple_size);
			}
		}
		// for right semi join, just mark the entry as found and move on. Propogation happens later
		if (ht.join_type != JoinType::RIGHT_SEMI && ht.join_type != JoinType::RIGHT_ANTI) {
			// matches were found
			// construct the result
			// on the LHS, we create a slice using the result vector
			result.Slice(left, result_vector, result_count);

			// on the RHS, we need to fetch the data from the hash table
			for (idx_t i = 0; i < ht.output_columns.size(); i++) {
				auto &vector = result.data[left.ColumnCount() + i];
				const auto output_col_idx = ht.output_columns[i];
				D_ASSERT(vector.GetType() == ht.layout.GetTypes()[output_col_idx]);
				GatherResult(vector, result_vector, result_count, output_col_idx);
			}
		}
		AdvancePointers();
	}
}

void ScanStructure::ScanKeyMatches(DataChunk &keys) {
	// the semi-join, anti-join and mark-join we handle a differently from the inner join
	// since there can be at most STANDARD_VECTOR_SIZE results
	// we handle the entire chunk in one call to Next().
	// for every pointer, we keep chasing pointers and doing comparisons.
	// this results in a boolean array indicating whether or not the tuple has a match
	SelectionVector match_sel(STANDARD_VECTOR_SIZE), no_match_sel(STANDARD_VECTOR_SIZE);
	while (this->count > 0) {
		// resolve the equality_predicates for the current set of pointers
		idx_t match_count = ResolvePredicates(keys, match_sel, &no_match_sel);
		idx_t no_match_count = this->count - match_count;

		// mark each of the matches as found
		for (idx_t i = 0; i < match_count; i++) {
			found_match[match_sel.get_index(i)] = true;
		}
		// continue searching for the ones where we did not find a match yet
		AdvancePointers(no_match_sel, no_match_count);
	}
}

template <bool MATCH>
void ScanStructure::NextSemiOrAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	D_ASSERT(left.ColumnCount() == result.ColumnCount());
	D_ASSERT(keys.size() == left.size());
	// create the selection vector from the matches that were found
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t result_count = 0;
	for (idx_t i = 0; i < keys.size(); i++) {
		if (found_match[i] == MATCH) {
			// part of the result
			sel.set_index(result_count++, i);
		}
	}
	// construct the final result
	if (result_count > 0) {
		// we only return the columns on the left side
		// reference the columns of the left side from the result
		result.Slice(left, sel, result_count);
	} else {
		D_ASSERT(result.size() == 0);
	}
}

void ScanStructure::NextSemiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// first scan for key matches
	ScanKeyMatches(keys);
	// then construct the result from all tuples with a match
	NextSemiOrAntiJoin<true>(keys, left, result);

	finished = true;
}

void ScanStructure::NextAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// first scan for key matches
	ScanKeyMatches(keys);
	// then construct the result from all tuples that did not find a match
	NextSemiOrAntiJoin<false>(keys, left, result);

	finished = true;
}

void ScanStructure::ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &child, DataChunk &result) {
	// for the initial set of columns we just reference the left side
	result.SetCardinality(child);
	for (idx_t i = 0; i < child.ColumnCount(); i++) {
		result.data[i].Reference(child.data[i]);
	}
	auto &mark_vector = result.data.back();
	mark_vector.SetVectorType(VectorType::FLAT_VECTOR);
	// first we set the NULL values from the join keys
	// if there is any NULL in the keys, the result is NULL
	auto bool_result = FlatVector::GetData<bool>(mark_vector);
	auto &mask = FlatVector::Validity(mark_vector);
	for (idx_t col_idx = 0; col_idx < join_keys.ColumnCount(); col_idx++) {
		if (ht.null_values_are_equal[col_idx]) {
			continue;
		}
		UnifiedVectorFormat jdata;
		join_keys.data[col_idx].ToUnifiedFormat(join_keys.size(), jdata);
		if (!jdata.validity.AllValid()) {
			for (idx_t i = 0; i < join_keys.size(); i++) {
				auto jidx = jdata.sel->get_index(i);
				mask.Set(i, jdata.validity.RowIsValidUnsafe(jidx));
			}
		}
	}
	// now set the remaining entries to either true or false based on whether a match was found
	if (found_match) {
		for (idx_t i = 0; i < child.size(); i++) {
			bool_result[i] = found_match[i];
		}
	} else {
		memset(bool_result, 0, sizeof(bool) * child.size());
	}
	// if the right side contains NULL values, the result of any FALSE becomes NULL
	if (ht.has_null) {
		for (idx_t i = 0; i < child.size(); i++) {
			if (!bool_result[i]) {
				mask.SetInvalid(i);
			}
		}
	}
}

void ScanStructure::NextMarkJoin(DataChunk &keys, DataChunk &input, DataChunk &result) {
	D_ASSERT(result.ColumnCount() == input.ColumnCount() + 1);
	D_ASSERT(result.data.back().GetType() == LogicalType::BOOLEAN);
	// this method should only be called for a non-empty HT
	D_ASSERT(ht.Count() > 0);

	ScanKeyMatches(keys);
	if (ht.correlated_mark_join_info.correlated_types.empty()) {
		ConstructMarkJoinResult(keys, input, result);
	} else {
		auto &info = ht.correlated_mark_join_info;
		lock_guard<mutex> mj_lock(info.mj_lock);

		// there are correlated columns
		// first we fetch the counts from the aggregate hashtable corresponding to these entries
		D_ASSERT(keys.ColumnCount() == info.group_chunk.ColumnCount() + 1);
		info.group_chunk.SetCardinality(keys);
		for (idx_t i = 0; i < info.group_chunk.ColumnCount(); i++) {
			info.group_chunk.data[i].Reference(keys.data[i]);
		}
		info.correlated_counts->FetchAggregates(info.group_chunk, info.result_chunk);

		// for the initial set of columns we just reference the left side
		result.SetCardinality(input);
		for (idx_t i = 0; i < input.ColumnCount(); i++) {
			result.data[i].Reference(input.data[i]);
		}
		// create the result matching vector
		auto &last_key = keys.data.back();
		auto &result_vector = result.data.back();
		// first set the nullmask based on whether or not there were NULL values in the join key
		result_vector.SetVectorType(VectorType::FLAT_VECTOR);
		auto bool_result = FlatVector::GetData<bool>(result_vector);
		auto &mask = FlatVector::Validity(result_vector);
		switch (last_key.GetVectorType()) {
		case VectorType::CONSTANT_VECTOR:
			if (ConstantVector::IsNull(last_key)) {
				mask.SetAllInvalid(input.size());
			}
			break;
		case VectorType::FLAT_VECTOR:
			mask.Copy(FlatVector::Validity(last_key), input.size());
			break;
		default: {
			UnifiedVectorFormat kdata;
			last_key.ToUnifiedFormat(keys.size(), kdata);
			for (idx_t i = 0; i < input.size(); i++) {
				auto kidx = kdata.sel->get_index(i);
				mask.Set(i, kdata.validity.RowIsValid(kidx));
			}
			break;
		}
		}

		auto count_star = FlatVector::GetData<int64_t>(info.result_chunk.data[0]);
		auto count = FlatVector::GetData<int64_t>(info.result_chunk.data[1]);
		// set the entries to either true or false based on whether a match was found
		for (idx_t i = 0; i < input.size(); i++) {
			D_ASSERT(count_star[i] >= count[i]);
			bool_result[i] = found_match ? found_match[i] : false;
			if (!bool_result[i] && count_star[i] > count[i]) {
				// RHS has NULL value and result is false: set to null
				mask.SetInvalid(i);
			}
			if (count_star[i] == 0) {
				// count == 0, set nullmask to false (we know the result is false now)
				mask.SetValid(i);
			}
		}
	}
	finished = true;
}

void ScanStructure::NextLeftJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// a LEFT OUTER JOIN is identical to an INNER JOIN except all tuples that do
	// not have a match must return at least one tuple (with the right side set
	// to NULL in every column)
	NextInnerJoin(keys, left, result);
	if (result.size() == 0) {
		// no entries left from the normal join
		// fill in the result of the remaining left tuples
		// together with NULL values on the right-hand side
		idx_t remaining_count = 0;
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < left.size(); i++) {
			if (!found_match[i]) {
				sel.set_index(remaining_count++, i);
			}
		}
		if (remaining_count > 0) {
			// have remaining tuples
			// slice the left side with tuples that did not find a match
			result.Slice(left, sel, remaining_count);

			// now set the right side to NULL
			for (idx_t i = left.ColumnCount(); i < result.ColumnCount(); i++) {
				Vector &vec = result.data[i];
				vec.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(vec, true);
			}
		}
		finished = true;
	}
}

void ScanStructure::NextSingleJoin(DataChunk &keys, DataChunk &input, DataChunk &result) {
	// single join
	// this join is similar to the semi join except that
	// (1) we actually return data from the RHS and
	// (2) we return NULL for that data if there is no match
	idx_t result_count = 0;
	SelectionVector result_sel(STANDARD_VECTOR_SIZE);
	SelectionVector match_sel(STANDARD_VECTOR_SIZE), no_match_sel(STANDARD_VECTOR_SIZE);
	while (this->count > 0) {
		// resolve the equality_predicates for the current set of pointers
		idx_t match_count = ResolvePredicates(keys, match_sel, &no_match_sel);
		idx_t no_match_count = this->count - match_count;

		// mark each of the matches as found
		for (idx_t i = 0; i < match_count; i++) {
			// found a match for this index
			auto index = match_sel.get_index(i);
			found_match[index] = true;
			result_sel.set_index(result_count++, index);
		}
		// continue searching for the ones where we did not find a match yet
		AdvancePointers(no_match_sel, no_match_count);
	}
	// reference the columns of the left side from the result
	D_ASSERT(input.ColumnCount() > 0);
	for (idx_t i = 0; i < input.ColumnCount(); i++) {
		result.data[i].Reference(input.data[i]);
	}
	// now fetch the data from the RHS
	for (idx_t i = 0; i < ht.output_columns.size(); i++) {
		auto &vector = result.data[input.ColumnCount() + i];
		// set NULL entries for every entry that was not found
		for (idx_t j = 0; j < input.size(); j++) {
			if (!found_match[j]) {
				FlatVector::SetNull(vector, j, true);
			}
		}
		const auto output_col_idx = ht.output_columns[i];
		D_ASSERT(vector.GetType() == ht.layout.GetTypes()[output_col_idx]);
		GatherResult(vector, result_sel, result_sel, result_count, output_col_idx);
	}
	result.SetCardinality(input.size());

	// like the SEMI, ANTI and MARK join types, the SINGLE join only ever does one pass over the HT per input chunk
	finished = true;
}

void JoinHashTable::ScanFullOuter(JoinHTScanState &state, Vector &addresses, DataChunk &result) {
	// scan the HT starting from the current position and check which rows from the build side did not find a match
	auto key_locations = FlatVector::GetData<data_ptr_t>(addresses);
	idx_t found_entries = 0;

	auto &iterator = state.iterator;
	if (iterator.Done()) {
		return;
	}

	// When scanning Full Outer for right semi joins, we only propagate matches that have true
	// Right Semi Joins do not propagate values during the probe phase, since we do not want to
	// duplicate RHS rows.
	bool match_propagation_value = false;
	if (join_type == JoinType::RIGHT_SEMI) {
		match_propagation_value = true;
	}

	const auto row_locations = iterator.GetRowLocations();
	do {
		const auto count = iterator.GetCurrentChunkCount();
		for (idx_t i = state.offset_in_chunk; i < count; i++) {
			auto found_match = Load<bool>(row_locations[i] + tuple_size);
			if (found_match == match_propagation_value) {
				key_locations[found_entries++] = row_locations[i];
				if (found_entries == STANDARD_VECTOR_SIZE) {
					state.offset_in_chunk = i + 1;
					break;
				}
			}
		}
		if (found_entries == STANDARD_VECTOR_SIZE) {
			break;
		}
		state.offset_in_chunk = 0;
	} while (iterator.Next());

	// now gather from the found rows
	if (found_entries == 0) {
		return;
	}
	result.SetCardinality(found_entries);

	idx_t left_column_count = result.ColumnCount() - output_columns.size();
	if (join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI) {
		left_column_count = 0;
	}
	const auto &sel_vector = *FlatVector::IncrementalSelectionVector();
	// set the left side as a constant NULL
	for (idx_t i = 0; i < left_column_count; i++) {
		Vector &vec = result.data[i];
		vec.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(vec, true);
	}

	// gather the values from the RHS
	for (idx_t i = 0; i < output_columns.size(); i++) {
		auto &vector = result.data[left_column_count + i];
		const auto output_col_idx = output_columns[i];
		D_ASSERT(vector.GetType() == layout.GetTypes()[output_col_idx]);
		data_collection->Gather(addresses, sel_vector, found_entries, output_col_idx, vector, sel_vector, nullptr);
	}
}

idx_t JoinHashTable::FillWithHTOffsets(JoinHTScanState &state, Vector &addresses) {
	// iterate over HT
	auto key_locations = FlatVector::GetData<data_ptr_t>(addresses);
	idx_t key_count = 0;

	auto &iterator = state.iterator;
	const auto row_locations = iterator.GetRowLocations();
	do {
		const auto count = iterator.GetCurrentChunkCount();
		for (idx_t i = 0; i < count; i++) {
			key_locations[key_count + i] = row_locations[i];
		}
		key_count += count;
	} while (iterator.Next());

	return key_count;
}

idx_t JoinHashTable::GetTotalSize(const vector<idx_t> &partition_sizes, const vector<idx_t> &partition_counts,
                                  idx_t &max_partition_size, idx_t &max_partition_count) const {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);

	idx_t total_size = 0;
	idx_t total_count = 0;
	idx_t max_partition_ht_size = 0;
	max_partition_size = 0;
	max_partition_count = 0;
	for (idx_t i = 0; i < num_partitions; i++) {
		total_size += partition_sizes[i];
		total_count += partition_counts[i];

		auto partition_size = partition_sizes[i] + PointerTableSize(partition_counts[i]);
		if (partition_size > max_partition_ht_size) {
			max_partition_ht_size = partition_size;
			max_partition_size = partition_sizes[i];
			max_partition_count = partition_counts[i];
		}
	}

	if (total_count == 0) {
		return 0;
	}

	return total_size + PointerTableSize(total_count);
}

idx_t JoinHashTable::GetTotalSize(vector<unique_ptr<JoinHashTable>> &local_hts, idx_t &max_partition_size,
                                  idx_t &max_partition_count) const {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	vector<idx_t> partition_sizes(num_partitions, 0);
	vector<idx_t> partition_counts(num_partitions, 0);
	for (auto &ht : local_hts) {
		ht->GetSinkCollection().GetSizesAndCounts(partition_sizes, partition_counts);
	}

	return GetTotalSize(partition_sizes, partition_counts, max_partition_size, max_partition_count);
}

idx_t JoinHashTable::GetRemainingSize() {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	auto &partitions = sink_collection->GetPartitions();

	idx_t count = 0;
	idx_t data_size = 0;
	for (idx_t partition_idx = partition_end; partition_idx < num_partitions; partition_idx++) {
		count += partitions[partition_idx]->Count();
		data_size += partitions[partition_idx]->SizeInBytes();
	}

	return data_size + PointerTableSize(count);
}

void JoinHashTable::Unpartition() {
	data_collection = sink_collection->GetUnpartitioned();
}

void JoinHashTable::SetRepartitionRadixBits(vector<unique_ptr<JoinHashTable>> &local_hts, const idx_t max_ht_size,
                                            const idx_t max_partition_size, const idx_t max_partition_count) {
	D_ASSERT(max_partition_size + PointerTableSize(max_partition_count) > max_ht_size);

	const auto max_added_bits = RadixPartitioning::MAX_RADIX_BITS - radix_bits;
	idx_t added_bits = 1;
	for (; added_bits < max_added_bits; added_bits++) {
		double partition_multiplier = RadixPartitioning::NumberOfPartitions(added_bits);

		auto new_estimated_size = double(max_partition_size) / partition_multiplier;
		auto new_estimated_count = double(max_partition_count) / partition_multiplier;
		auto new_estimated_ht_size = new_estimated_size + PointerTableSize(new_estimated_count);

		if (new_estimated_ht_size <= double(max_ht_size) / 4) {
			// Aim for an estimated partition size of max_ht_size / 4
			break;
		}
	}
	radix_bits += added_bits;
	sink_collection =
	    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, radix_bits, layout.ColumnCount() - 1);
}

void JoinHashTable::Repartition(JoinHashTable &global_ht) {
	auto new_sink_collection =
	    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, global_ht.radix_bits, layout.ColumnCount() - 1);
	sink_collection->Repartition(*new_sink_collection);
	sink_collection = std::move(new_sink_collection);
	global_ht.Merge(*this);
}

void JoinHashTable::Reset() {
	data_collection->Reset();
	finalized = false;
}

bool JoinHashTable::PrepareExternalFinalize(const idx_t max_ht_size) {
	if (finalized) {
		Reset();
	}

	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	if (partition_end == num_partitions) {
		return false;
	}

	// Start where we left off
	auto &partitions = sink_collection->GetPartitions();
	partition_start = partition_end;

	// Determine how many partitions we can do next (at least one)
	idx_t count = 0;
	idx_t data_size = 0;
	idx_t partition_idx;
	for (partition_idx = partition_start; partition_idx < num_partitions; partition_idx++) {
		auto incl_count = count + partitions[partition_idx]->Count();
		auto incl_data_size = data_size + partitions[partition_idx]->SizeInBytes();
		auto incl_ht_size = incl_data_size + PointerTableSize(incl_count);
		if (count > 0 && incl_ht_size > max_ht_size) {
			break;
		}
		count = incl_count;
		data_size = incl_data_size;
	}
	partition_end = partition_idx;

	// Move the partitions to the main data collection
	for (partition_idx = partition_start; partition_idx < partition_end; partition_idx++) {
		data_collection->Combine(*partitions[partition_idx]);
	}
	D_ASSERT(Count() == count);

	return true;
}

static void CreateSpillChunk(DataChunk &spill_chunk, DataChunk &keys, DataChunk &payload, Vector &hashes) {
	spill_chunk.Reset();
	idx_t spill_col_idx = 0;
	for (idx_t col_idx = 0; col_idx < keys.ColumnCount(); col_idx++) {
		spill_chunk.data[col_idx].Reference(keys.data[col_idx]);
	}
	spill_col_idx += keys.ColumnCount();
	for (idx_t col_idx = 0; col_idx < payload.data.size(); col_idx++) {
		spill_chunk.data[spill_col_idx + col_idx].Reference(payload.data[col_idx]);
	}
	spill_col_idx += payload.ColumnCount();
	spill_chunk.data[spill_col_idx].Reference(hashes);
}

unique_ptr<ScanStructure> JoinHashTable::ProbeAndSpill(DataChunk &keys, TupleDataChunkState &key_state,
                                                       ProbeState &probe_state, DataChunk &payload,
                                                       ProbeSpill &probe_spill, ProbeSpillLocalAppendState &spill_state,
                                                       DataChunk &spill_chunk) {
	// hash all the keys
	Vector hashes(LogicalType::HASH);
	Hash(keys, *FlatVector::IncrementalSelectionVector(), keys.size(), hashes);

	// find out which keys we can match with the current pinned partitions
	SelectionVector true_sel;
	SelectionVector false_sel;
	true_sel.Initialize();
	false_sel.Initialize();
	auto true_count = RadixPartitioning::Select(hashes, FlatVector::IncrementalSelectionVector(), keys.size(),
	                                            radix_bits, partition_end, &true_sel, &false_sel);
	auto false_count = keys.size() - true_count;

	CreateSpillChunk(spill_chunk, keys, payload, hashes);

	// can't probe these values right now, append to spill
	spill_chunk.Slice(false_sel, false_count);
	spill_chunk.Verify();
	probe_spill.Append(spill_chunk, spill_state);

	// slice the stuff we CAN probe right now
	hashes.Slice(true_sel, true_count);
	keys.Slice(true_sel, true_count);
	payload.Slice(true_sel, true_count);

	const SelectionVector *current_sel;
	auto ss = InitializeScanStructure(keys, key_state, current_sel);
	if (ss->count == 0) {
		return ss;
	}

	// now initialize the pointers of the scan structure based on the hashes
	GetRowPointers(keys, key_state, probe_state, hashes, *current_sel, ss->count, ss->pointers);

	// create the selection vector linking to only non-empty entries
	ss->InitializeSelectionVector(current_sel);

	return ss;
}

ProbeSpill::ProbeSpill(JoinHashTable &ht, ClientContext &context, const vector<LogicalType> &probe_types)
    : ht(ht), context(context), probe_types(probe_types) {
	global_partitions =
	    make_uniq<RadixPartitionedColumnData>(context, probe_types, ht.radix_bits, probe_types.size() - 1);
	column_ids.reserve(probe_types.size());
	for (column_t column_id = 0; column_id < probe_types.size(); column_id++) {
		column_ids.emplace_back(column_id);
	}
}

ProbeSpillLocalState ProbeSpill::RegisterThread() {
	ProbeSpillLocalAppendState result;
	lock_guard<mutex> guard(lock);
	local_partitions.emplace_back(global_partitions->CreateShared());
	local_partition_append_states.emplace_back(make_uniq<PartitionedColumnDataAppendState>());
	local_partitions.back()->InitializeAppendState(*local_partition_append_states.back());

	result.local_partition = local_partitions.back().get();
	result.local_partition_append_state = local_partition_append_states.back().get();
	return result;
}

void ProbeSpill::Append(DataChunk &chunk, ProbeSpillLocalAppendState &local_state) {
	local_state.local_partition->Append(*local_state.local_partition_append_state, chunk);
}

void ProbeSpill::Finalize() {
	D_ASSERT(local_partitions.size() == local_partition_append_states.size());
	for (idx_t i = 0; i < local_partition_append_states.size(); i++) {
		local_partitions[i]->FlushAppendState(*local_partition_append_states[i]);
	}
	for (auto &local_partition : local_partitions) {
		global_partitions->Combine(*local_partition);
	}
	local_partitions.clear();
	local_partition_append_states.clear();
}

void ProbeSpill::PrepareNextProbe() {
	auto &partitions = global_partitions->GetPartitions();
	if (partitions.empty() || ht.partition_start == partitions.size()) {
		// Can't probe, just make an empty one
		global_spill_collection =
		    make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), probe_types);
	} else {
		// Move specific partitions to the global spill collection
		global_spill_collection = std::move(partitions[ht.partition_start]);
		for (idx_t i = ht.partition_start + 1; i < ht.partition_end; i++) {
			auto &partition = partitions[i];
			if (global_spill_collection->Count() == 0) {
				global_spill_collection = std::move(partition);
			} else {
				global_spill_collection->Combine(*partition);
			}
		}
	}
	consumer = make_uniq<ColumnDataConsumer>(*global_spill_collection, column_ids);
	consumer->InitializeScan();
}

} // namespace duckdb
