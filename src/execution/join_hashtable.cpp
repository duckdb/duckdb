#include "duckdb/execution/join_hashtable.hpp"

#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

namespace duckdb {

using ScanStructure = JoinHashTable::ScanStructure;

JoinHashTable::JoinHashTable(BufferManager &buffer_manager, vector<JoinCondition> &conditions,
                             vector<LogicalType> btypes, JoinType type)
    : row_chunk(buffer_manager), build_types(move(btypes)), equality_size(0), condition_size(0), build_size(0),
      tuple_size(0), join_type(type), finalized(false), has_null(false) {
	for (auto &condition : conditions) {
		D_ASSERT(condition.left->return_type == condition.right->return_type);
		auto type = condition.left->return_type;
		auto type_size = GetTypeIdSize(type.InternalType());
		if (condition.comparison == ExpressionType::COMPARE_EQUAL) {
			// all equality conditions should be at the front
			// all other conditions at the back
			// this assert checks that
			D_ASSERT(equality_types.size() == condition_types.size());
			equality_types.push_back(type);
			equality_size += type_size;
		}
		predicates.push_back(condition.comparison);
		null_values_are_equal.push_back(condition.null_values_are_equal);
		D_ASSERT(!condition.null_values_are_equal ||
		         (condition.null_values_are_equal && condition.comparison == ExpressionType::COMPARE_EQUAL));

		condition_types.push_back(type);
		condition_size += type_size;
	}
	// at least one equality is necessary
	D_ASSERT(equality_types.size() > 0);
	// size of nullmask bitset for each column that is pushed in
	row_chunk.nullmask_size = (build_types.size() + 7) / 8;
	for (idx_t i = 0; i < build_types.size(); i++) {
		build_size += GetTypeIdSize(build_types[i].InternalType());
	}
	tuple_size = condition_size + build_size;
	pointer_offset = row_chunk.nullmask_size + tuple_size;
	// entry size is the tuple size and the size of the hash/next pointer
	row_chunk.entry_size = row_chunk.nullmask_size + tuple_size + MaxValue(sizeof(hash_t), sizeof(uintptr_t));
	if (IsRightOuterJoin(join_type)) {
		// full/right outer joins need an extra bool to keep track of whether or not a tuple has found a matching entry
		// we place the bool before the NEXT pointer
		row_chunk.entry_size += sizeof(bool);
		pointer_offset += sizeof(bool);
	}
	// compute the per-block capacity of this HT
	row_chunk.block_capacity =
	    MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_ALLOC_SIZE / row_chunk.entry_size) + 1);
}

JoinHashTable::~JoinHashTable() {
}

void JoinHashTable::ApplyBitmask(Vector &hashes, idx_t count) {
	if (hashes.vector_type == VectorType::CONSTANT_VECTOR) {
		D_ASSERT(!ConstantVector::IsNull(hashes));
		auto indices = ConstantVector::GetData<hash_t>(hashes);
		*indices = *indices & bitmask;
	} else {
		hashes.Normalify(count);
		auto indices = FlatVector::GetData<hash_t>(hashes);
		for (idx_t i = 0; i < count; i++) {
			indices[i] &= bitmask;
		}
	}
}

void JoinHashTable::ApplyBitmask(Vector &hashes, const SelectionVector &sel, idx_t count, Vector &pointers) {
	VectorData hdata;
	hashes.Orrify(count, hdata);

	auto hash_data = (hash_t *)hdata.data;
	auto result_data = FlatVector::GetData<data_ptr_t *>(pointers);
	auto main_ht = (data_ptr_t *)hash_map->node->buffer;
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
<<<<<<< HEAD
=======
template <class T>
static void TemplatedSerializeVData(VectorData &vdata, const SelectionVector &sel, idx_t count,
                                    data_ptr_t key_locations[]) {
	auto source = (T *)vdata.data;
	if (vdata.nullmask->any()) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);

			auto target = (T *)key_locations[i];
			T value = (*vdata.nullmask)[source_idx] ? NullValue<T>() : source[source_idx];
			Store<T>(value, (data_ptr_t)target);
			key_locations[i] += sizeof(T);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);

			auto target = (T *)key_locations[i];
			Store<T>(source[source_idx], (data_ptr_t)target);
			key_locations[i] += sizeof(T);
		}
	}
}
>>>>>>> master

static void InitializeOuterJoin(idx_t count, data_ptr_t key_locations[]) {
	for (idx_t i = 0; i < count; i++) {
		auto target = (bool *)key_locations[i];
		*target = false;
		key_locations[i] += sizeof(bool);
	}
}

<<<<<<< HEAD
=======
void JoinHashTable::SerializeVectorData(VectorData &vdata, PhysicalType type, const SelectionVector &sel, idx_t count,
                                        data_ptr_t key_locations[]) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedSerializeVData<int8_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::INT16:
		TemplatedSerializeVData<int16_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::INT32:
		TemplatedSerializeVData<int32_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::INT64:
		TemplatedSerializeVData<int64_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::UINT8:
		TemplatedSerializeVData<uint8_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::UINT16:
		TemplatedSerializeVData<uint16_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::UINT32:
		TemplatedSerializeVData<uint32_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::UINT64:
		TemplatedSerializeVData<uint64_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::INT128:
		TemplatedSerializeVData<hugeint_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::FLOAT:
		TemplatedSerializeVData<float>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::DOUBLE:
		TemplatedSerializeVData<double>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::HASH:
		TemplatedSerializeVData<hash_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::INTERVAL:
		TemplatedSerializeVData<interval_t>(vdata, sel, count, key_locations);
		break;
	case PhysicalType::VARCHAR: {
		StringHeap local_heap;
		auto source = (string_t *)vdata.data;
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);

			string_t new_val;
			if ((*vdata.nullmask)[source_idx]) {
				new_val = NullValue<string_t>();
			} else if (source[source_idx].IsInlined()) {
				new_val = source[source_idx];
			} else {
				new_val = local_heap.AddBlob(source[source_idx].GetDataUnsafe(), source[source_idx].GetSize());
			}
			Store<string_t>(new_val, key_locations[i]);
			key_locations[i] += sizeof(string_t);
		}
		lock_guard<mutex> append_lock(ht_lock);
		string_heap.MergeHeap(local_heap);
		break;
	}
	default:
		throw NotImplementedException("FIXME: unimplemented serialize");
	}
}

void JoinHashTable::SerializeVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t count,
                                    data_ptr_t key_locations[]) {
	VectorData vdata;
	v.Orrify(vcount, vdata);

	SerializeVectorData(vdata, v.type.InternalType(), sel, count, key_locations);
}

idx_t JoinHashTable::AppendToBlock(HTDataBlock &block, BufferHandle &handle, vector<BlockAppendEntry> &append_entries,
                                   idx_t remaining) {
	idx_t append_count = MinValue<idx_t>(remaining, block.capacity - block.count);
	auto dataptr = handle.node->buffer + block.count * entry_size;
	append_entries.emplace_back(dataptr, append_count);
	block.count += append_count;
	return append_count;
}

>>>>>>> master
static idx_t FilterNullValues(VectorData &vdata, const SelectionVector &sel, idx_t count, SelectionVector &result) {
	auto &nullmask = *vdata.nullmask;
	idx_t result_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto key_idx = vdata.sel->get_index(idx);
		if (!nullmask[key_idx]) {
			result.set_index(result_count++, idx);
		}
	}
	return result_count;
}

idx_t JoinHashTable::PrepareKeys(DataChunk &keys, unique_ptr<VectorData[]> &key_data,
                                 const SelectionVector *&current_sel, SelectionVector &sel, bool build_side) {
	key_data = keys.Orrify();

	// figure out which keys are NULL, and create a selection vector out of them
	current_sel = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
	idx_t added_count = keys.size();
	if (build_side && IsRightOuterJoin(join_type)) {
		// in case of a right or full outer join, we cannot remove NULL keys from the build side
		return added_count;
	}
	for (idx_t i = 0; i < keys.ColumnCount(); i++) {
		if (!null_values_are_equal[i]) {
			if (!key_data[i].nullmask->any()) {
				continue;
			}
			added_count = FilterNullValues(key_data[i], *current_sel, added_count, sel);
			// null values are NOT equal for this column, filter them out
			current_sel = &sel;
		}
	}
	return added_count;
}

void JoinHashTable::Build(DataChunk &keys, DataChunk &payload) {
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
		info.payload_chunk.SetCardinality(keys);
		info.payload_chunk.data[0].Reference(keys.data[info.correlated_types.size()]);
		info.correlated_counts->AddChunk(info.group_chunk, info.payload_chunk);
	}

	// prepare the keys for processing
	unique_ptr<VectorData[]> key_data;
	const SelectionVector *current_sel;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t added_count = PrepareKeys(keys, key_data, current_sel, sel, true);
	if (added_count < keys.size()) {
		has_null = true;
	}
	if (added_count == 0) {
		return;
	}

	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t nullmask_locations[STANDARD_VECTOR_SIZE];
	row_chunk.Build(added_count, key_locations, nullmask_locations);

	// hash the keys and obtain an entry in the list
	// note that we only hash the keys used in the equality comparison
	Vector hash_values(LogicalType::HASH);
	Hash(keys, *current_sel, added_count, hash_values);

	// serialize the keys to the key locations
	for (idx_t i = 0; i < keys.ColumnCount(); i++) {
		row_chunk.SerializeVectorData(key_data[i], keys.data[i].type.InternalType(), *current_sel, added_count, i,
		                              key_locations, nullmask_locations);
	}
	// now serialize the payload
	if (!build_types.empty()) {
		for (idx_t i = 0; i < payload.ColumnCount(); i++) {
			row_chunk.SerializeVector(payload.data[i], payload.size(), *current_sel, added_count,
			                          keys.ColumnCount() + i, key_locations, nullmask_locations);
		}
	}
	if (IsRightOuterJoin(join_type)) {
		// for FULL/RIGHT OUTER joins initialize the "found" boolean to false
		InitializeOuterJoin(added_count, key_locations);
	}
	row_chunk.SerializeVector(hash_values, payload.size(), *current_sel, added_count,
	                          keys.ColumnCount() + build_types.size() - 1, key_locations, nullmask_locations);
}

void JoinHashTable::InsertHashes(Vector &hashes, idx_t count, data_ptr_t key_locations[]) {
	D_ASSERT(hashes.type.id() == LogicalTypeId::HASH);

	// use bitmask to get position in array
	ApplyBitmask(hashes, count);

	hashes.Normalify(count);

	D_ASSERT(hashes.vector_type == VectorType::FLAT_VECTOR);
	auto pointers = (data_ptr_t *)hash_map->node->buffer;
	auto indices = FlatVector::GetData<hash_t>(hashes);
	for (idx_t i = 0; i < count; i++) {
		auto index = indices[i];
		// set prev in current key to the value (NOTE: this will be nullptr if
		// there is none)
		auto prev_pointer = (data_ptr_t *)(key_locations[i] + pointer_offset);
		Store<data_ptr_t>(pointers[index], (data_ptr_t)prev_pointer);

		// set pointer to current tuple
		pointers[index] = key_locations[i];
	}
}

void JoinHashTable::Finalize() {
	// the build has finished, now iterate over all the nodes and construct the final hash table
	// select a HT that has at least 50% empty space
	idx_t capacity = NextPowerOfTwo(MaxValue<idx_t>(size() * 2, (Storage::BLOCK_ALLOC_SIZE / sizeof(data_ptr_t)) + 1));
	// size needs to be a power of 2
	D_ASSERT((capacity & (capacity - 1)) == 0);
	bitmask = capacity - 1;

	// allocate the HT and initialize it with all-zero entries
	hash_map = row_chunk.buffer_manager.Allocate(capacity * sizeof(data_ptr_t));
	memset(hash_map->node->buffer, 0, capacity * sizeof(data_ptr_t));

	Vector hashes(LogicalType::HASH);
	auto hash_data = FlatVector::GetData<hash_t>(hashes);
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	// now construct the actual hash table; scan the nodes
	// as we can the nodes we pin all the blocks of the HT and keep them pinned until the HT is destroyed
	// this is so that we can keep pointers around to the blocks
	// FIXME: if we cannot keep everything pinned in memory, we could switch to an out-of-memory merge join or so
	for (auto &block : row_chunk.blocks) {
		auto handle = row_chunk.buffer_manager.Pin(block.block);
		data_ptr_t dataptr = handle->node->buffer;
		idx_t entry = 0;
		while (entry < block.count) {
			// fetch the next vector of entries from the blocks
			idx_t next = MinValue<idx_t>(STANDARD_VECTOR_SIZE, block.count - entry);
			for (idx_t i = 0; i < next; i++) {
				hash_data[i] = Load<hash_t>((data_ptr_t)(dataptr + pointer_offset));
				key_locations[i] = dataptr + row_chunk.nullmask_size;
				dataptr += row_chunk.entry_size;
			}
			// now insert into the hash table
			InsertHashes(hashes, next, key_locations);

			entry += next;
		}
		pinned_handles.push_back(move(handle));
	}

	finalized = true;
}

unique_ptr<ScanStructure> JoinHashTable::Probe(DataChunk &keys) {
	D_ASSERT(size() > 0); // should be handled before
	D_ASSERT(finalized);

	// set up the scan structure
	auto ss = make_unique<ScanStructure>(*this);

	if (join_type != JoinType::INNER) {
		ss->found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
		memset(ss->found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
	}

	// first prepare the keys for probing
	const SelectionVector *current_sel;
	ss->count = PrepareKeys(keys, ss->key_data, current_sel, ss->sel_vector, false);
	if (ss->count == 0) {
		return ss;
	}

	// hash all the keys
	Vector hashes(LogicalType::HASH);
	Hash(keys, *current_sel, ss->count, hashes);

	// now initialize the pointers of the scan structure based on the hashes
	ApplyBitmask(hashes, *current_sel, ss->count, ss->pointers);

	// create the selection vector linking to only non-empty entries
	idx_t count = 0;
	auto pointers = FlatVector::GetData<data_ptr_t>(ss->pointers);
	for (idx_t i = 0; i < ss->count; i++) {
		auto idx = current_sel->get_index(i);
		auto chain_pointer = (data_ptr_t *)(pointers[idx]);
		pointers[idx] = *chain_pointer;
		if (pointers[idx]) {
			ss->sel_vector.set_index(count++, idx);
		}
	}
	ss->count = count;
	return ss;
}

ScanStructure::ScanStructure(JoinHashTable &ht) : sel_vector(STANDARD_VECTOR_SIZE), ht(ht), finished(false) {
	pointers.Initialize(LogicalType::POINTER);
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
		throw Exception("Unhandled join type in JoinHashTable");
	}
}

template <bool NO_MATCH_SEL, class T, class OP>
static idx_t TemplatedGather(VectorData &vdata, Vector &pointers, const SelectionVector &current_sel, idx_t count,
                             idx_t offset, SelectionVector *match_sel, SelectionVector *no_match_sel,
                             idx_t &no_match_count) {
	idx_t result_count = 0;
	auto data = (T *)vdata.data;
	auto ptrs = FlatVector::GetData<uintptr_t>(pointers);
	for (idx_t i = 0; i < count; i++) {
		auto idx = current_sel.get_index(i);
		auto kidx = vdata.sel->get_index(idx);
		auto gdata = (T *)(ptrs[idx] + offset);
		T val = Load<T>((data_ptr_t)gdata);
		if ((*vdata.nullmask)[kidx]) {
			if (IsNullValue<T>(val)) {
				match_sel->set_index(result_count++, idx);
			} else {
				if (NO_MATCH_SEL) {
					no_match_sel->set_index(no_match_count++, idx);
				}
			}
		} else {
			if (OP::template Operation<T>(data[kidx], val)) {
				match_sel->set_index(result_count++, idx);
			} else {
				if (NO_MATCH_SEL) {
					no_match_sel->set_index(no_match_count++, idx);
				}
			}
		}
	}
	return result_count;
}

template <bool NO_MATCH_SEL, class OP>
static idx_t GatherSwitch(VectorData &data, PhysicalType type, Vector &pointers, const SelectionVector &current_sel,
                          idx_t count, idx_t offset, SelectionVector *match_sel, SelectionVector *no_match_sel,
                          idx_t &no_match_count) {
	switch (type) {
	case PhysicalType::UINT8:
		return TemplatedGather<NO_MATCH_SEL, uint8_t, OP>(data, pointers, current_sel, count, offset, match_sel,
		                                                  no_match_sel, no_match_count);
	case PhysicalType::UINT16:
		return TemplatedGather<NO_MATCH_SEL, uint16_t, OP>(data, pointers, current_sel, count, offset, match_sel,
		                                                   no_match_sel, no_match_count);
	case PhysicalType::UINT32:
		return TemplatedGather<NO_MATCH_SEL, uint32_t, OP>(data, pointers, current_sel, count, offset, match_sel,
		                                                   no_match_sel, no_match_count);
	case PhysicalType::UINT64:
		return TemplatedGather<NO_MATCH_SEL, uint64_t, OP>(data, pointers, current_sel, count, offset, match_sel,
		                                                   no_match_sel, no_match_count);
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedGather<NO_MATCH_SEL, int8_t, OP>(data, pointers, current_sel, count, offset, match_sel,
		                                                 no_match_sel, no_match_count);
	case PhysicalType::INT16:
		return TemplatedGather<NO_MATCH_SEL, int16_t, OP>(data, pointers, current_sel, count, offset, match_sel,
		                                                  no_match_sel, no_match_count);
	case PhysicalType::INT32:
		return TemplatedGather<NO_MATCH_SEL, int32_t, OP>(data, pointers, current_sel, count, offset, match_sel,
		                                                  no_match_sel, no_match_count);
	case PhysicalType::INT64:
		return TemplatedGather<NO_MATCH_SEL, int64_t, OP>(data, pointers, current_sel, count, offset, match_sel,
		                                                  no_match_sel, no_match_count);
	case PhysicalType::INT128:
		return TemplatedGather<NO_MATCH_SEL, hugeint_t, OP>(data, pointers, current_sel, count, offset, match_sel,
		                                                    no_match_sel, no_match_count);
	case PhysicalType::FLOAT:
		return TemplatedGather<NO_MATCH_SEL, float, OP>(data, pointers, current_sel, count, offset, match_sel,
		                                                no_match_sel, no_match_count);
	case PhysicalType::DOUBLE:
		return TemplatedGather<NO_MATCH_SEL, double, OP>(data, pointers, current_sel, count, offset, match_sel,
		                                                 no_match_sel, no_match_count);
	case PhysicalType::INTERVAL:
		return TemplatedGather<NO_MATCH_SEL, interval_t, OP>(data, pointers, current_sel, count, offset, match_sel,
		                                                     no_match_sel, no_match_count);
	case PhysicalType::VARCHAR:
		return TemplatedGather<NO_MATCH_SEL, string_t, OP>(data, pointers, current_sel, count, offset, match_sel,
		                                                   no_match_sel, no_match_count);
	default:
		throw NotImplementedException("Unimplemented type for GatherSwitch");
	}
}

template <bool NO_MATCH_SEL>
idx_t ScanStructure::ResolvePredicates(DataChunk &keys, SelectionVector *match_sel, SelectionVector *no_match_sel) {
	SelectionVector *current_sel = &this->sel_vector;
	idx_t remaining_count = this->count;
	idx_t offset = 0;
	idx_t no_match_count = 0;
	for (idx_t i = 0; i < ht.predicates.size(); i++) {
		auto internal_type = keys.data[i].type.InternalType();
		switch (ht.predicates[i]) {
		case ExpressionType::COMPARE_EQUAL:
			remaining_count =
			    GatherSwitch<NO_MATCH_SEL, Equals>(key_data[i], internal_type, this->pointers, *current_sel,
			                                       remaining_count, offset, match_sel, no_match_sel, no_match_count);
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			remaining_count =
			    GatherSwitch<NO_MATCH_SEL, NotEquals>(key_data[i], internal_type, this->pointers, *current_sel,
			                                          remaining_count, offset, match_sel, no_match_sel, no_match_count);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			remaining_count = GatherSwitch<NO_MATCH_SEL, GreaterThan>(key_data[i], internal_type, this->pointers,
			                                                          *current_sel, remaining_count, offset, match_sel,
			                                                          no_match_sel, no_match_count);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			remaining_count = GatherSwitch<NO_MATCH_SEL, GreaterThanEquals>(key_data[i], internal_type, this->pointers,
			                                                                *current_sel, remaining_count, offset,
			                                                                match_sel, no_match_sel, no_match_count);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			remaining_count =
			    GatherSwitch<NO_MATCH_SEL, LessThan>(key_data[i], internal_type, this->pointers, *current_sel,
			                                         remaining_count, offset, match_sel, no_match_sel, no_match_count);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			remaining_count = GatherSwitch<NO_MATCH_SEL, LessThanEquals>(key_data[i], internal_type, this->pointers,
			                                                             *current_sel, remaining_count, offset,
			                                                             match_sel, no_match_sel, no_match_count);
			break;
		default:
			throw NotImplementedException("Unimplemented comparison type for join");
		}
		if (remaining_count == 0) {
			break;
		}
		current_sel = match_sel;
		offset += GetTypeIdSize(internal_type);
	}
	return remaining_count;
}

idx_t ScanStructure::ResolvePredicates(DataChunk &keys, SelectionVector &match_sel, SelectionVector &no_match_sel) {
	return ResolvePredicates<true>(keys, &match_sel, &no_match_sel);
}

idx_t ScanStructure::ResolvePredicates(DataChunk &keys, SelectionVector &match_sel) {
	return ResolvePredicates<false>(keys, &match_sel, nullptr);
}

idx_t ScanStructure::ScanInnerJoin(DataChunk &keys, SelectionVector &result_vector) {
	while (true) {
		// resolve the predicates for this set of keys
		idx_t result_count = ResolvePredicates(keys, result_vector);

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
		auto chain_pointer = (data_ptr_t *)(ptrs[idx] + ht.pointer_offset);
		ptrs[idx] = Load<data_ptr_t>((data_ptr_t)chain_pointer);
		if (ptrs[idx]) {
			this->sel_vector.set_index(new_count++, idx);
		}
	}
	this->count = new_count;
}

void ScanStructure::AdvancePointers() {
	AdvancePointers(this->sel_vector, this->count);
}

template <class T>
static void TemplatedGatherResult(Vector &result, uintptr_t *pointers, const SelectionVector &result_vector,
                                  const SelectionVector &sel_vector, idx_t count, idx_t offset) {
	auto rdata = FlatVector::GetData<T>(result);
	auto &nullmask = FlatVector::Nullmask(result);
	for (idx_t i = 0; i < count; i++) {
		auto ridx = result_vector.get_index(i);
		auto pidx = sel_vector.get_index(i);
		T hdata = Load<T>((data_ptr_t)(pointers[pidx] + offset));
		if (IsNullValue<T>(hdata)) {
			nullmask[ridx] = true;
		} else {
			rdata[ridx] = hdata;
		}
	}
}

static void GatherResultVector(Vector &result, const SelectionVector &result_vector, uintptr_t *ptrs,
                               const SelectionVector &sel_vector, idx_t count, idx_t &offset) {
	result.vector_type = VectorType::FLAT_VECTOR;
	switch (result.type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedGatherResult<int8_t>(result, ptrs, result_vector, sel_vector, count, offset);
		break;
	case PhysicalType::INT16:
		TemplatedGatherResult<int16_t>(result, ptrs, result_vector, sel_vector, count, offset);
		break;
	case PhysicalType::INT32:
		TemplatedGatherResult<int32_t>(result, ptrs, result_vector, sel_vector, count, offset);
		break;
	case PhysicalType::INT64:
		TemplatedGatherResult<int64_t>(result, ptrs, result_vector, sel_vector, count, offset);
		break;
	case PhysicalType::UINT8:
		TemplatedGatherResult<uint8_t>(result, ptrs, result_vector, sel_vector, count, offset);
		break;
	case PhysicalType::UINT16:
		TemplatedGatherResult<uint16_t>(result, ptrs, result_vector, sel_vector, count, offset);
		break;
	case PhysicalType::UINT32:
		TemplatedGatherResult<uint32_t>(result, ptrs, result_vector, sel_vector, count, offset);
		break;
	case PhysicalType::UINT64:
		TemplatedGatherResult<uint64_t>(result, ptrs, result_vector, sel_vector, count, offset);
		break;
	case PhysicalType::INT128:
		TemplatedGatherResult<hugeint_t>(result, ptrs, result_vector, sel_vector, count, offset);
		break;
	case PhysicalType::FLOAT:
		TemplatedGatherResult<float>(result, ptrs, result_vector, sel_vector, count, offset);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGatherResult<double>(result, ptrs, result_vector, sel_vector, count, offset);
		break;
	case PhysicalType::INTERVAL:
		TemplatedGatherResult<interval_t>(result, ptrs, result_vector, sel_vector, count, offset);
		break;
	case PhysicalType::VARCHAR:
		TemplatedGatherResult<string_t>(result, ptrs, result_vector, sel_vector, count, offset);
		break;
	default:
		throw NotImplementedException("Unimplemented type for ScanStructure::GatherResult");
	}
	offset += GetTypeIdSize(result.type.InternalType());
}

void ScanStructure::GatherResult(Vector &result, const SelectionVector &result_vector,
                                 const SelectionVector &sel_vector, idx_t count, idx_t &offset) {
	auto ptrs = FlatVector::GetData<uintptr_t>(pointers);
	GatherResultVector(result, result_vector, ptrs, sel_vector, count, offset);
}

void ScanStructure::GatherResult(Vector &result, const SelectionVector &sel_vector, idx_t count, idx_t &offset) {
	GatherResult(result, FlatVector::INCREMENTAL_SELECTION_VECTOR, sel_vector, count, offset);
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
			auto ptrs = FlatVector::GetData<uintptr_t>(pointers);
			for (idx_t i = 0; i < result_count; i++) {
				auto idx = result_vector.get_index(i);
				auto chain_pointer = (data_ptr_t *)(ptrs[idx] + ht.tuple_size);
				auto target = (bool *)chain_pointer;
				*target = true;
			}
		}
		// matches were found
		// construct the result
		// on the LHS, we create a slice using the result vector
		result.Slice(left, result_vector, result_count);

		// on the RHS, we need to fetch the data from the hash table
		idx_t offset = ht.condition_size;
		for (idx_t i = 0; i < ht.build_types.size(); i++) {
			auto &vector = result.data[left.ColumnCount() + i];
			D_ASSERT(vector.type == ht.build_types[i]);
			GatherResult(vector, result_vector, result_count, offset);
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
		idx_t match_count = ResolvePredicates(keys, match_sel, no_match_sel);
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
	mark_vector.vector_type = VectorType::FLAT_VECTOR;
	// first we set the NULL values from the join keys
	// if there is any NULL in the keys, the result is NULL
	auto bool_result = FlatVector::GetData<bool>(mark_vector);
	auto &nullmask = FlatVector::Nullmask(mark_vector);
	for (idx_t col_idx = 0; col_idx < join_keys.ColumnCount(); col_idx++) {
		if (ht.null_values_are_equal[col_idx]) {
			continue;
		}
		VectorData jdata;
		join_keys.data[col_idx].Orrify(join_keys.size(), jdata);
		if (jdata.nullmask->any()) {
			for (idx_t i = 0; i < join_keys.size(); i++) {
				auto jidx = jdata.sel->get_index(i);
				nullmask[i] = (*jdata.nullmask)[jidx];
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
				nullmask[i] = true;
			}
		}
	}
}

void ScanStructure::NextMarkJoin(DataChunk &keys, DataChunk &input, DataChunk &result) {
	D_ASSERT(result.ColumnCount() == input.ColumnCount() + 1);
	D_ASSERT(result.data.back().type == LogicalType::BOOLEAN);
	// this method should only be called for a non-empty HT
	D_ASSERT(ht.size() > 0);

	ScanKeyMatches(keys);
	if (ht.correlated_mark_join_info.correlated_types.empty()) {
		ConstructMarkJoinResult(keys, input, result);
	} else {
		auto &info = ht.correlated_mark_join_info;
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
		result_vector.vector_type = VectorType::FLAT_VECTOR;
		auto bool_result = FlatVector::GetData<bool>(result_vector);
		auto &nullmask = FlatVector::Nullmask(result_vector);
		switch (last_key.vector_type) {
		case VectorType::CONSTANT_VECTOR:
			if (ConstantVector::IsNull(last_key)) {
				nullmask.set();
			}
			break;
		case VectorType::FLAT_VECTOR:
			nullmask = FlatVector::Nullmask(last_key);
			break;
		default: {
			VectorData kdata;
			last_key.Orrify(keys.size(), kdata);
			for (idx_t i = 0; i < input.size(); i++) {
				auto kidx = kdata.sel->get_index(i);
				nullmask[i] = (*kdata.nullmask)[kidx];
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
				nullmask[i] = true;
			}
			if (count_star[i] == 0) {
				// count == 0, set nullmask to false (we know the result is false now)
				nullmask[i] = false;
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
				result.data[i].vector_type = VectorType::CONSTANT_VECTOR;
				ConstantVector::SetNull(result.data[i], true);
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
		idx_t match_count = ResolvePredicates(keys, match_sel, no_match_sel);
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
	idx_t offset = ht.condition_size;
	for (idx_t i = 0; i < ht.build_types.size(); i++) {
		auto &vector = result.data[input.ColumnCount() + i];
		// set NULL entries for every entry that was not found
		auto &nullmask = FlatVector::Nullmask(vector);
		nullmask.set();
		for (idx_t j = 0; j < result_count; j++) {
			nullmask[result_sel.get_index(j)] = false;
		}
		// for the remaining values we fetch the values
		GatherResult(vector, result_sel, result_sel, result_count, offset);
	}
	result.SetCardinality(input.size());

	// like the SEMI, ANTI and MARK join types, the SINGLE join only ever does one pass over the HT per input chunk
	finished = true;
}

void JoinHashTable::ScanFullOuter(DataChunk &result, JoinHTScanState &state) {
	// scan the HT starting from the current position and check which rows from the build side did not find a match
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	idx_t found_entries = 0;
	for (; state.block_position < row_chunk.blocks.size(); state.block_position++, state.position = 0) {
		auto &block = row_chunk.blocks[state.block_position];
		auto &handle = pinned_handles[state.block_position];
		auto baseptr = handle->node->buffer;
		for (; state.position < block.count; state.position++) {
			auto tuple_base = baseptr + state.position * row_chunk.entry_size;
			auto found_match = (bool *)(tuple_base + tuple_size);
			if (!*found_match) {
				key_locations[found_entries++] = tuple_base;
				if (found_entries == STANDARD_VECTOR_SIZE) {
					state.position++;
					break;
				}
			}
		}
		if (found_entries == STANDARD_VECTOR_SIZE) {
			break;
		}
	}
	result.SetCardinality(found_entries);
	if (found_entries > 0) {
		idx_t left_column_count = result.ColumnCount() - build_types.size();
		// set the left side as a constant NULL
		for (idx_t i = 0; i < left_column_count; i++) {
			result.data[i].vector_type = VectorType::CONSTANT_VECTOR;
			ConstantVector::SetNull(result.data[i], true);
		}
		// gather the values from the RHS
		idx_t offset = condition_size;
		for (idx_t i = 0; i < build_types.size(); i++) {
			auto &vector = result.data[left_column_count + i];
			D_ASSERT(vector.type == build_types[i]);
			GatherResultVector(vector, FlatVector::INCREMENTAL_SELECTION_VECTOR, (uintptr_t *)key_locations,
			                   FlatVector::INCREMENTAL_SELECTION_VECTOR, found_entries, offset);
		}
	}
}

} // namespace duckdb
