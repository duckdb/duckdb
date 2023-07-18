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

JoinHashTable::JoinHashTable(BufferManager &buffer_manager_p, const vector<JoinCondition> &conditions_p,
                             vector<LogicalType> btypes, JoinType type_p)
    : buffer_manager(buffer_manager_p), conditions(conditions_p), build_types(std::move(btypes)), entry_size(0),
      tuple_size(0), vfound(Value::BOOLEAN(false)), join_type(type_p), finalized(false), has_null(false),
      external(false), radix_bits(4), partition_start(0), partition_end(0) {
	for (auto &condition : conditions) {
		D_ASSERT(condition.left->return_type == condition.right->return_type);
		auto type = condition.left->return_type;
		if (condition.comparison == ExpressionType::COMPARE_EQUAL ||
		    condition.comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM ||
		    condition.comparison == ExpressionType::COMPARE_DISTINCT_FROM) {
			// all equality conditions should be at the front
			// all other conditions at the back
			// this assert checks that
			D_ASSERT(equality_types.size() == condition_types.size());
			equality_types.push_back(type);
		}

		predicates.push_back(condition.comparison);
		null_values_are_equal.push_back(condition.comparison == ExpressionType::COMPARE_DISTINCT_FROM ||
		                                condition.comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM);

		condition_types.push_back(type);
	}
	// at least one equality is necessary
	D_ASSERT(!equality_types.empty());

	// Types for the layout
	vector<LogicalType> layout_types(condition_types);
	layout_types.insert(layout_types.end(), build_types.begin(), build_types.end());
	if (IsRightOuterJoin(join_type)) {
		// full/right outer joins need an extra bool to keep track of whether or not a tuple has found a matching entry
		// we place the bool before the NEXT pointer
		layout_types.emplace_back(LogicalType::BOOLEAN);
	}
	layout_types.emplace_back(LogicalType::HASH);
	layout.Initialize(layout_types, false);

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

void JoinHashTable::ApplyBitmask(Vector &hashes, idx_t count) {
	if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		D_ASSERT(!ConstantVector::IsNull(hashes));
		auto indices = ConstantVector::GetData<hash_t>(hashes);
		*indices = *indices & bitmask;
	} else {
		hashes.Flatten(count);
		auto indices = FlatVector::GetData<hash_t>(hashes);
		for (idx_t i = 0; i < count; i++) {
			indices[i] &= bitmask;
		}
	}
}

void JoinHashTable::ApplyBitmask(Vector &hashes, const SelectionVector &sel, idx_t count, Vector &pointers) {
	UnifiedVectorFormat hdata;
	hashes.ToUnifiedFormat(count, hdata);

	auto hash_data = UnifiedVectorFormat::GetData<hash_t>(hdata);
	auto result_data = FlatVector::GetData<data_ptr_t *>(pointers);
	auto main_ht = reinterpret_cast<data_ptr_t *>(hash_map.get());
	for (idx_t i = 0; i < count; i++) {
		auto rindex = sel.get_index(i);
		auto hindex = hdata.sel->get_index(rindex);
		auto hash = hash_data[hindex];
		result_data[rindex] = main_ht + (hash & bitmask);
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

idx_t JoinHashTable::PrepareKeys(DataChunk &keys, unsafe_unique_array<UnifiedVectorFormat> &key_data,
                                 const SelectionVector *&current_sel, SelectionVector &sel, bool build_side) {
	key_data = keys.ToUnifiedFormat();

	// figure out which keys are NULL, and create a selection vector out of them
	current_sel = FlatVector::IncrementalSelectionVector();
	idx_t added_count = keys.size();
	if (build_side && IsRightOuterJoin(join_type)) {
		// in case of a right or full outer join, we cannot remove NULL keys from the build side
		return added_count;
	}
	for (idx_t i = 0; i < keys.ColumnCount(); i++) {
		if (!null_values_are_equal[i]) {
			if (key_data[i].validity.AllValid()) {
				continue;
			}
			added_count = FilterNullValues(key_data[i], *current_sel, added_count, sel);
			// null values are NOT equal for this column, filter them out
			current_sel = &sel;
		}
	}
	return added_count;
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

	// prepare the keys for processing
	unsafe_unique_array<UnifiedVectorFormat> key_data;
	const SelectionVector *current_sel;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t added_count = PrepareKeys(keys, key_data, current_sel, sel, true);
	if (added_count < keys.size()) {
		has_null = true;
	}
	if (added_count == 0) {
		return;
	}

	// hash the keys and obtain an entry in the list
	// note that we only hash the keys used in the equality comparison
	Vector hash_values(LogicalType::HASH);
	Hash(keys, *current_sel, added_count, hash_values);

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
	if (IsRightOuterJoin(join_type)) {
		// for FULL/RIGHT OUTER joins initialize the "found" boolean to false
		source_chunk.data[col_offset].Reference(vfound);
		col_offset++;
	}
	source_chunk.data[col_offset].Reference(hash_values);
	source_chunk.SetCardinality(keys);

	if (added_count < keys.size()) {
		source_chunk.Slice(*current_sel, added_count);
	}
	sink_collection->Append(append_state, source_chunk);
}

template <bool PARALLEL>
static inline void InsertHashesLoop(atomic<data_ptr_t> pointers[], const hash_t indices[], const idx_t count,
                                    const data_ptr_t key_locations[], const idx_t pointer_offset) {
	for (idx_t i = 0; i < count; i++) {
		const auto index = indices[i];
		if (PARALLEL) {
			data_ptr_t head;
			do {
				head = pointers[index];
				Store<data_ptr_t>(head, key_locations[i] + pointer_offset);
			} while (!std::atomic_compare_exchange_weak(&pointers[index], &head, key_locations[i]));
		} else {
			// set prev in current key to the value (NOTE: this will be nullptr if there is none)
			Store<data_ptr_t>(pointers[index], key_locations[i] + pointer_offset);

			// set pointer to current tuple
			pointers[index] = key_locations[i];
		}
	}
}

void JoinHashTable::InsertHashes(Vector &hashes, idx_t count, data_ptr_t key_locations[], bool parallel) {
	D_ASSERT(hashes.GetType().id() == LogicalType::HASH);

	// use bitmask to get position in array
	ApplyBitmask(hashes, count);

	hashes.Flatten(count);
	D_ASSERT(hashes.GetVectorType() == VectorType::FLAT_VECTOR);

	auto pointers = reinterpret_cast<atomic<data_ptr_t> *>(hash_map.get());
	auto indices = FlatVector::GetData<hash_t>(hashes);

	if (parallel) {
		InsertHashesLoop<true>(pointers, indices, count, key_locations, pointer_offset);
	} else {
		InsertHashesLoop<false>(pointers, indices, count, key_locations, pointer_offset);
	}
}

void JoinHashTable::InitializePointerTable() {
	idx_t capacity = PointerTableCapacity(Count());
	D_ASSERT(IsPowerOfTwo(capacity));

	if (hash_map.get()) {
		// There is already a hash map
		auto current_capacity = hash_map.GetSize() / sizeof(data_ptr_t);
		if (capacity > current_capacity) {
			// Need more space
			hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(data_ptr_t));
		} else {
			// Just use the current hash map
			capacity = current_capacity;
		}
	} else {
		// Allocate a hash map
		hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(data_ptr_t));
	}
	D_ASSERT(hash_map.GetSize() == capacity * sizeof(data_ptr_t));

	// initialize HT with all-zero entries
	std::fill_n(reinterpret_cast<data_ptr_t *>(hash_map.get()), capacity, nullptr);

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
	do {
		const auto count = iterator.GetCurrentChunkCount();
		for (idx_t i = 0; i < count; i++) {
			hash_data[i] = Load<hash_t>(row_locations[i] + pointer_offset);
		}
		InsertHashes(hashes, count, row_locations, parallel);
	} while (iterator.Next());
}

unique_ptr<ScanStructure> JoinHashTable::InitializeScanStructure(DataChunk &keys, const SelectionVector *&current_sel) {
	D_ASSERT(Count() > 0); // should be handled before
	D_ASSERT(finalized);

	// set up the scan structure
	auto ss = make_uniq<ScanStructure>(*this);

	if (join_type != JoinType::INNER) {
		ss->found_match = make_unsafe_uniq_array<bool>(STANDARD_VECTOR_SIZE);
		memset(ss->found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
	}

	// first prepare the keys for probing
	ss->count = PrepareKeys(keys, ss->key_data, current_sel, ss->sel_vector, false);
	return ss;
}

unique_ptr<ScanStructure> JoinHashTable::Probe(DataChunk &keys, Vector *precomputed_hashes) {
	const SelectionVector *current_sel;
	auto ss = InitializeScanStructure(keys, current_sel);
	if (ss->count == 0) {
		return ss;
	}

	if (precomputed_hashes) {
		ApplyBitmask(*precomputed_hashes, *current_sel, ss->count, ss->pointers);
	} else {
		// hash all the keys
		Vector hashes(LogicalType::HASH);
		Hash(keys, *current_sel, ss->count, hashes);

		// now initialize the pointers of the scan structure based on the hashes
		ApplyBitmask(hashes, *current_sel, ss->count, ss->pointers);
	}

	// create the selection vector linking to only non-empty entries
	ss->InitializeSelectionVector(current_sel);

	return ss;
}

ScanStructure::ScanStructure(JoinHashTable &ht)
    : pointers(LogicalType::POINTER), sel_vector(STANDARD_VECTOR_SIZE), ht(ht), finished(false) {
}

void ScanStructure::Next(DataChunk &keys, DataChunk &left, DataChunk &result) {
	if (finished) {
		return;
	}
	switch (ht.join_type) {
	case JoinType::INNER:
	case JoinType::RIGHT:
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

idx_t ScanStructure::ResolvePredicates(DataChunk &keys, SelectionVector &match_sel, SelectionVector *no_match_sel) {
	// Start with the scan selection
	for (idx_t i = 0; i < this->count; ++i) {
		match_sel.set_index(i, this->sel_vector.get_index(i));
	}
	idx_t no_match_count = 0;

	return RowOperations::Match(keys, key_data.get(), ht.layout, pointers, ht.predicates, match_sel, this->count,
	                            no_match_sel, no_match_count);
}

idx_t ScanStructure::ScanInnerJoin(DataChunk &keys, SelectionVector &result_vector) {
	while (true) {
		// resolve the predicates for this set of keys
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
		ptrs[idx] = Load<data_ptr_t>(ptrs[idx]);
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
	ht.data_collection->Gather(pointers, sel_vector, count, col_no, result, result_vector);
}

void ScanStructure::GatherResult(Vector &result, const SelectionVector &sel_vector, const idx_t count,
                                 const idx_t col_idx) {
	GatherResult(result, *FlatVector::IncrementalSelectionVector(), sel_vector, count, col_idx);
}

void ScanStructure::NextInnerJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	D_ASSERT(result.ColumnCount() == left.ColumnCount() + ht.build_types.size());
	if (this->count == 0) {
		// no pointers left to chase
		return;
	}

	SelectionVector result_vector(STANDARD_VECTOR_SIZE);

	idx_t result_count = ScanInnerJoin(keys, result_vector);
	if (result_count > 0) {
		if (IsRightOuterJoin(ht.join_type)) {
			// full/right outer join: mark join matches as FOUND in the HT
			auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);
			for (idx_t i = 0; i < result_count; i++) {
				auto idx = result_vector.get_index(i);
				// NOTE: threadsan reports this as a data race because this can be set concurrently by separate threads
				// Technically it is, but it does not matter, since the only value that can be written is "true"
				Store<bool>(true, ptrs[idx] + ht.tuple_size);
			}
		}
		// matches were found
		// construct the result
		// on the LHS, we create a slice using the result vector
		result.Slice(left, result_vector, result_count);

		// on the RHS, we need to fetch the data from the hash table
		for (idx_t i = 0; i < ht.build_types.size(); i++) {
			auto &vector = result.data[left.ColumnCount() + i];
			D_ASSERT(vector.GetType() == ht.build_types[i]);
			GatherResult(vector, result_vector, result_count, i + ht.condition_types.size());
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
		// resolve the predicates for the current set of pointers
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
		// resolve the predicates for the current set of pointers
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
	for (idx_t i = 0; i < ht.build_types.size(); i++) {
		auto &vector = result.data[input.ColumnCount() + i];
		// set NULL entries for every entry that was not found
		for (idx_t j = 0; j < input.size(); j++) {
			if (!found_match[j]) {
				FlatVector::SetNull(vector, j, true);
			}
		}
		// for the remaining values we fetch the values
		GatherResult(vector, result_sel, result_sel, result_count, i + ht.condition_types.size());
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

	const auto row_locations = iterator.GetRowLocations();
	do {
		const auto count = iterator.GetCurrentChunkCount();
		for (idx_t i = state.offset_in_chunk; i < count; i++) {
			auto found_match = Load<bool>(row_locations[i] + tuple_size);
			if (!found_match) {
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
	idx_t left_column_count = result.ColumnCount() - build_types.size();
	const auto &sel_vector = *FlatVector::IncrementalSelectionVector();
	// set the left side as a constant NULL
	for (idx_t i = 0; i < left_column_count; i++) {
		Vector &vec = result.data[i];
		vec.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(vec, true);
	}

	// gather the values from the RHS
	for (idx_t i = 0; i < build_types.size(); i++) {
		auto &vector = result.data[left_column_count + i];
		D_ASSERT(vector.GetType() == build_types[i]);
		const auto col_no = condition_types.size() + i;
		data_collection->Gather(addresses, sel_vector, found_entries, col_no, vector, sel_vector);
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

bool JoinHashTable::RequiresExternalJoin(ClientConfig &config, vector<unique_ptr<JoinHashTable>> &local_hts) {
	total_count = 0;
	idx_t data_size = 0;
	for (auto &ht : local_hts) {
		auto &local_sink_collection = ht->GetSinkCollection();
		total_count += local_sink_collection.Count();
		data_size += local_sink_collection.SizeInBytes();
	}

	if (total_count == 0) {
		return false;
	}

	if (config.force_external) {
		// Do 1 round per partition if forcing external join to test all code paths
		const auto r = RadixPartitioning::NumberOfPartitions(radix_bits);
		auto data_size_per_round = (data_size + r - 1) / r;
		auto count_per_round = (total_count + r - 1) / r;
		max_ht_size = data_size_per_round + PointerTableSize(count_per_round);
		external = true;
	} else {
		auto ht_size = data_size + PointerTableSize(total_count);
		external = ht_size > max_ht_size;
	}
	return external;
}

void JoinHashTable::Unpartition() {
	for (auto &partition : sink_collection->GetPartitions()) {
		data_collection->Combine(*partition);
	}
}

bool JoinHashTable::RequiresPartitioning(ClientConfig &config, vector<unique_ptr<JoinHashTable>> &local_hts) {
	D_ASSERT(total_count != 0);
	D_ASSERT(external);

	idx_t num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	vector<idx_t> partition_counts(num_partitions, 0);
	vector<idx_t> partition_sizes(num_partitions, 0);
	for (auto &ht : local_hts) {
		const auto &local_partitions = ht->GetSinkCollection().GetPartitions();
		for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
			auto &local_partition = local_partitions[partition_idx];
			partition_counts[partition_idx] += local_partition->Count();
			partition_sizes[partition_idx] += local_partition->SizeInBytes();
		}
	}

	// Figure out if we can fit all single partitions in memory
	idx_t max_partition_idx = 0;
	idx_t max_partition_size = 0;
	for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
		const auto &partition_count = partition_counts[partition_idx];
		const auto &partition_size = partition_sizes[partition_idx];
		auto partition_ht_size = partition_size + PointerTableSize(partition_count);
		if (partition_ht_size > max_partition_size) {
			max_partition_size = partition_ht_size;
			max_partition_idx = partition_idx;
		}
	}

	if (config.force_external || max_partition_size > max_ht_size) {
		const auto partition_count = partition_counts[max_partition_idx];
		const auto partition_size = partition_sizes[max_partition_idx];

		const auto max_added_bits = RadixPartitioning::MAX_RADIX_BITS - radix_bits;
		idx_t added_bits = config.force_external ? 2 : 1;
		for (; added_bits < max_added_bits; added_bits++) {
			double partition_multiplier = RadixPartitioning::NumberOfPartitions(added_bits);

			auto new_estimated_count = double(partition_count) / partition_multiplier;
			auto new_estimated_size = double(partition_size) / partition_multiplier;
			auto new_estimated_ht_size = new_estimated_size + PointerTableSize(new_estimated_count);

			if (config.force_external || new_estimated_ht_size <= double(max_ht_size) / 4) {
				// Aim for an estimated partition size of max_ht_size / 4
				break;
			}
		}
		radix_bits += added_bits;
		sink_collection =
		    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, radix_bits, layout.ColumnCount() - 1);
		return true;
	} else {
		return false;
	}
}

void JoinHashTable::Partition(JoinHashTable &global_ht) {
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

bool JoinHashTable::PrepareExternalFinalize() {
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

unique_ptr<ScanStructure> JoinHashTable::ProbeAndSpill(DataChunk &keys, DataChunk &payload, ProbeSpill &probe_spill,
                                                       ProbeSpillLocalAppendState &spill_state,
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
	auto ss = InitializeScanStructure(keys, current_sel);
	if (ss->count == 0) {
		return ss;
	}

	// now initialize the pointers of the scan structure based on the hashes
	ApplyBitmask(hashes, *current_sel, ss->count, ss->pointers);

	// create the selection vector linking to only non-empty entries
	ss->InitializeSelectionVector(current_sel);

	return ss;
}

ProbeSpill::ProbeSpill(JoinHashTable &ht, ClientContext &context, const vector<LogicalType> &probe_types)
    : ht(ht), context(context), probe_types(probe_types) {
	auto remaining_count = ht.GetSinkCollection().Count();
	auto remaining_data_size = ht.GetSinkCollection().SizeInBytes();
	auto remaining_ht_size = remaining_data_size + ht.PointerTableSize(remaining_count);
	if (remaining_ht_size <= ht.max_ht_size) {
		// No need to partition as we will only have one more probe round
		partitioned = false;
	} else {
		// More than one probe round to go, so we need to partition
		partitioned = true;
		global_partitions =
		    make_uniq<RadixPartitionedColumnData>(context, probe_types, ht.radix_bits, probe_types.size() - 1);
	}
	column_ids.reserve(probe_types.size());
	for (column_t column_id = 0; column_id < probe_types.size(); column_id++) {
		column_ids.emplace_back(column_id);
	}
}

ProbeSpillLocalState ProbeSpill::RegisterThread() {
	ProbeSpillLocalAppendState result;
	lock_guard<mutex> guard(lock);
	if (partitioned) {
		local_partitions.emplace_back(global_partitions->CreateShared());
		local_partition_append_states.emplace_back(make_uniq<PartitionedColumnDataAppendState>());
		local_partitions.back()->InitializeAppendState(*local_partition_append_states.back());

		result.local_partition = local_partitions.back().get();
		result.local_partition_append_state = local_partition_append_states.back().get();
	} else {
		local_spill_collections.emplace_back(
		    make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), probe_types));
		local_spill_append_states.emplace_back(make_uniq<ColumnDataAppendState>());
		local_spill_collections.back()->InitializeAppend(*local_spill_append_states.back());

		result.local_spill_collection = local_spill_collections.back().get();
		result.local_spill_append_state = local_spill_append_states.back().get();
	}
	return result;
}

void ProbeSpill::Append(DataChunk &chunk, ProbeSpillLocalAppendState &local_state) {
	if (partitioned) {
		local_state.local_partition->Append(*local_state.local_partition_append_state, chunk);
	} else {
		local_state.local_spill_collection->Append(*local_state.local_spill_append_state, chunk);
	}
}

void ProbeSpill::Finalize() {
	if (partitioned) {
		D_ASSERT(local_partitions.size() == local_partition_append_states.size());
		for (idx_t i = 0; i < local_partition_append_states.size(); i++) {
			local_partitions[i]->FlushAppendState(*local_partition_append_states[i]);
		}
		for (auto &local_partition : local_partitions) {
			global_partitions->Combine(*local_partition);
		}
		local_partitions.clear();
		local_partition_append_states.clear();
	} else {
		if (local_spill_collections.empty()) {
			global_spill_collection =
			    make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), probe_types);
		} else {
			global_spill_collection = std::move(local_spill_collections[0]);
			for (idx_t i = 1; i < local_spill_collections.size(); i++) {
				global_spill_collection->Combine(*local_spill_collections[i]);
			}
		}
		local_spill_collections.clear();
		local_spill_append_states.clear();
	}
}

void ProbeSpill::PrepareNextProbe() {
	if (partitioned) {
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
	}
	consumer = make_uniq<ColumnDataConsumer>(*global_spill_collection, column_ids);
	consumer->InitializeScan();
}

} // namespace duckdb
