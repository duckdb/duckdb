#include "duckdb/execution/join_hashtable.hpp"

#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

using namespace duckdb;
using namespace std;

using ScanStructure = JoinHashTable::ScanStructure;

static void SerializeChunk(DataChunk &source, data_ptr_t targets[]) {
	Vector target_vector(TypeId::POINTER, (data_ptr_t)targets);
	target_vector.count = source.size();
	target_vector.sel_vector = source.sel_vector;

	index_t offset = 0;
	for (index_t i = 0; i < source.column_count; i++) {
		VectorOperations::Scatter::SetAll(source.data[i], target_vector, true, offset);
		offset += GetTypeIdSize(source.data[i].type);
	}
}

JoinHashTable::JoinHashTable(BufferManager &buffer_manager, vector<JoinCondition> &conditions,
                             vector<TypeId> build_types, JoinType type)
    : buffer_manager(buffer_manager), build_types(build_types), equality_size(0), condition_size(0), build_size(0),
      entry_size(0), tuple_size(0), join_type(type), finalized(false), has_null(false), count(0) {
	for (auto &condition : conditions) {
		assert(condition.left->return_type == condition.right->return_type);
		auto type = condition.left->return_type;
		auto type_size = GetTypeIdSize(type);
		if (condition.comparison == ExpressionType::COMPARE_EQUAL) {
			// all equality conditions should be at the front
			// all other conditions at the back
			// this assert checks that
			assert(equality_types.size() == condition_types.size());
			equality_types.push_back(type);
			equality_size += type_size;
		}
		predicates.push_back(condition.comparison);
		null_values_are_equal.push_back(condition.null_values_are_equal);
		assert(!condition.null_values_are_equal ||
		       (condition.null_values_are_equal && condition.comparison == ExpressionType::COMPARE_EQUAL));

		condition_types.push_back(type);
		condition_size += type_size;
	}
	// at least one equality is necessary
	assert(equality_types.size() > 0);

	if (type == JoinType::ANTI || type == JoinType::SEMI || type == JoinType::MARK) {
		// for ANTI, SEMI and MARK join, we only need to store the keys
		build_size = 0;
	} else {
		// otherwise we need to store the entire build side for reconstruction
		// purposes
		for (index_t i = 0; i < build_types.size(); i++) {
			build_size += GetTypeIdSize(build_types[i]);
		}
	}
	tuple_size = condition_size + build_size;
	// entry size is the tuple size and the size of the hash/next pointer
	entry_size = tuple_size + std::max(sizeof(uint64_t), sizeof(void *));
	// compute the per-block capacity of this HT
	block_capacity = std::max((index_t)STANDARD_VECTOR_SIZE, (Storage::BLOCK_ALLOC_SIZE / entry_size) + 1);
}

JoinHashTable::~JoinHashTable() {
	if (hash_map) {
		auto hash_id = hash_map->block_id;
		hash_map.reset();
		buffer_manager.DestroyBuffer(hash_id);
	}
	pinned_handles.clear();
	for (auto &block : blocks) {
		buffer_manager.DestroyBuffer(block.block_id);
	}
}

void JoinHashTable::ApplyBitmask(Vector &hashes) {
	auto indices = (uint64_t *)hashes.GetData();
	if (hashes.vector_type == VectorType::CONSTANT_VECTOR) {
		assert(!hashes.nullmask[0]);
		indices[0] = indices[0] & bitmask;
	} else {
		hashes.Normalify();
		VectorOperations::Exec(hashes, [&](index_t i, index_t k) { indices[i] = indices[i] & bitmask; });
	}
}

void JoinHashTable::Hash(DataChunk &keys, Vector &hashes) {
	VectorOperations::Hash(keys.data[0], hashes);
	for (index_t i = 1; i < equality_types.size(); i++) {
		VectorOperations::CombineHash(hashes, keys.data[i]);
	}
}

static index_t CreateNotNullSelVector(DataChunk &keys, sel_t *not_null_sel_vector) {
	sel_t *sel_vector = keys.data[0].sel_vector;
	index_t result_count = keys.size();
	// first we loop over all the columns and figure out where the
	for (index_t i = 0; i < keys.column_count; i++) {
		keys.data[i].sel_vector = sel_vector;
		keys.data[i].count = result_count;
		result_count = Vector::NotNullSelVector(keys.data[i], not_null_sel_vector, sel_vector, nullptr);
	}
	// now assign the final count and selection vectors
	for (index_t i = 0; i < keys.column_count; i++) {
		keys.data[i].sel_vector = sel_vector;
		keys.data[i].count = result_count;
	}
	keys.sel_vector = sel_vector;
	return result_count;
}

index_t JoinHashTable::AppendToBlock(HTDataBlock &block, BufferHandle &handle, Vector &key_data,
                                     data_ptr_t key_locations[], data_ptr_t tuple_locations[],
                                     data_ptr_t hash_locations[], index_t remaining) {
	index_t append_count = std::min(remaining, block.capacity - block.count);
	auto dataptr = handle.node->buffer + block.count * entry_size;
	index_t offset = key_data.count - remaining;
	VectorOperations::Exec(
	    key_data,
	    [&](index_t i, index_t k) {
		    // key is stored at the start
		    key_locations[i] = dataptr;
		    // after that the build-side tuple is stored
		    tuple_locations[i] = dataptr + condition_size;
		    // the hash is stored at the end, after the key and build
		    hash_locations[i] = dataptr + tuple_size;
		    dataptr += entry_size;
	    },
	    offset, append_count);
	block.count += append_count;
	return append_count;
}

void JoinHashTable::Build(DataChunk &keys, DataChunk &payload) {
	assert(!finalized);
	assert(keys.size() == payload.size());
	if (keys.size() == 0) {
		return;
	}
	count += keys.size();
	// move strings to the string heap
	keys.MoveStringsToHeap(string_heap);
	payload.MoveStringsToHeap(string_heap);

	keys.Normalify();
	payload.Normalify();

	// for any columns for which null values are equal, fill the NullMask
	assert(keys.column_count == null_values_are_equal.size());
	bool null_values_equal_for_all = true;
	for (index_t i = 0; i < keys.column_count; i++) {
		if (null_values_are_equal[i]) {
			VectorOperations::FillNullMask(keys.data[i]);
		} else {
			null_values_equal_for_all = false;
		}
	}
	// special case: correlated mark join
	if (join_type == JoinType::MARK && correlated_mark_join_info.correlated_types.size() > 0) {
		auto &info = correlated_mark_join_info;
		// Correlated MARK join
		// for the correlated mark join we need to keep track of COUNT(*) and COUNT(COLUMN) for each of the correlated
		// columns push into the aggregate hash table
		assert(info.correlated_counts);
		for (index_t i = 0; i < info.correlated_types.size(); i++) {
			info.group_chunk.data[i].Reference(keys.data[i]);
		}
		info.payload_chunk.data[0].Reference(keys.data[info.correlated_types.size()]);
		info.payload_chunk.data[1].Reference(keys.data[info.correlated_types.size()]);
		info.payload_chunk.data[0].type = info.payload_chunk.data[1].type = TypeId::INT64;
		info.payload_chunk.sel_vector = info.group_chunk.sel_vector = info.group_chunk.data[0].sel_vector;
		info.correlated_counts->AddChunk(info.group_chunk, info.payload_chunk);
	}
	sel_t not_null_sel_vector[STANDARD_VECTOR_SIZE];
	if (!null_values_equal_for_all) {
		// if any columns are <<not>> supposed to have NULL values are equal:
		// first create a selection vector of the non-null values in the keys
		// because in a join, any NULL value can never find a matching tuple
		index_t initial_keys_size = keys.size();
		index_t not_null_count;
		not_null_count = CreateNotNullSelVector(keys, not_null_sel_vector);
		if (not_null_count != initial_keys_size) {
			// the hashtable contains null values in the keys!
			// set the property in the HT to true
			// this is required for the mark join
			has_null = true;
			// now assign the new count and sel_vector to the payload as well
			for (index_t i = 0; i < payload.column_count; i++) {
				payload.data[i].count = not_null_count;
				payload.data[i].sel_vector = keys.data[0].sel_vector;
			}
			payload.sel_vector = keys.data[0].sel_vector;
		}
		if (not_null_count == 0) {
			return;
		}
	}

	vector<unique_ptr<BufferHandle>> handles;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t tuple_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t hash_locations[STANDARD_VECTOR_SIZE];
	// first allocate space of where to serialize the keys and payload columns
	index_t remaining = keys.data[0].count;
	// first append to the last block (if any)
	if (blocks.size() != 0) {
		auto &last_block = blocks.back();
		if (last_block.count < last_block.capacity) {
			// last block has space: pin the buffer of this block
			auto handle = buffer_manager.Pin(last_block.block_id);
			// now append to the block
			index_t append_count = AppendToBlock(last_block, *handle, keys.data[0], key_locations, tuple_locations,
			                                     hash_locations, remaining);
			remaining -= append_count;
			handles.push_back(move(handle));
		}
	}
	while (remaining > 0) {
		// now for the remaining data, allocate new buffers to store the data and append there
		auto handle = buffer_manager.Allocate(block_capacity * entry_size);

		HTDataBlock new_block;
		new_block.count = 0;
		new_block.capacity = block_capacity;
		new_block.block_id = handle->block_id;

		index_t append_count =
		    AppendToBlock(new_block, *handle, keys.data[0], key_locations, tuple_locations, hash_locations, remaining);
		remaining -= append_count;
		handles.push_back(move(handle));
		blocks.push_back(new_block);
	}

	// hash the keys and obtain an entry in the list
	// note that we only hash the keys used in the equality comparison
	vector<TypeId> hash_types = {TypeId::HASH};
	DataChunk hash_chunk;
	hash_chunk.Initialize(hash_types);
	Hash(keys, hash_chunk.data[0]);

	// serialize the key, payload and hash values to these locations
	SerializeChunk(keys, key_locations);
	if (build_size > 0) {
		SerializeChunk(payload, tuple_locations);
	}
	SerializeChunk(hash_chunk, hash_locations);
}

uint64_t NextPowerOfTwo(uint64_t v) {
	v--;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	v |= v >> 32;
	v++;
	return v;
}

void JoinHashTable::InsertHashes(Vector &hashes, data_ptr_t key_locations[]) {
	assert(hashes.type == TypeId::HASH);

	// use bitmask to get position in array
	ApplyBitmask(hashes);

	auto pointers = (data_ptr_t *)hash_map->node->buffer;
	auto indices = (index_t *)hashes.GetData();
	// now fill in the entries
	VectorOperations::Exec(hashes, [&](index_t i, index_t k) {
		auto index = indices[i];
		// set prev in current key to the value (NOTE: this will be nullptr if
		// there is none)
		auto prev_pointer = (data_ptr_t *)(key_locations[i] + tuple_size);
		*prev_pointer = pointers[index];

		// set pointer to current tuple
		pointers[index] = key_locations[i];
	});
}

void JoinHashTable::Finalize() {
	// the build has finished, now iterate over all the nodes and construct the final hash table
	// select a HT that has at least 50% empty space
	index_t capacity =
	    NextPowerOfTwo(std::max(count * 2, (index_t)(Storage::BLOCK_ALLOC_SIZE / sizeof(data_ptr_t)) + 1));
	// size needs to be a power of 2
	assert((capacity & (capacity - 1)) == 0);
	bitmask = capacity - 1;

	// allocate the HT and initialize it with all-zero entries
	hash_map = buffer_manager.Allocate(capacity * sizeof(data_ptr_t));
	memset(hash_map->node->buffer, 0, capacity * sizeof(data_ptr_t));

	Vector hashes(TypeId::HASH, true, false);
	auto hash_data = (uint64_t *)hashes.GetData();
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	// now construct the actual hash table; scan the nodes
	// as we can the nodes we pin all the blocks of the HT and keep them pinned until the HT is destroyed
	// this is so that we can keep pointers around to the blocks
	// FIXME: if we cannot keep everything pinned in memory, we could switch to an out-of-memory merge join or so
	for (auto &block : blocks) {
		auto handle = buffer_manager.Pin(block.block_id);
		data_ptr_t dataptr = handle->node->buffer;
		index_t entry = 0;
		while (entry < block.count) {
			// fetch the next vector of entries from the blocks
			index_t next = std::min((index_t)STANDARD_VECTOR_SIZE, block.count - entry);
			for (index_t i = 0; i < next; i++) {
				hash_data[i] = *((uint64_t *)(dataptr + tuple_size));
				key_locations[i] = dataptr;
				dataptr += entry_size;
			}
			hashes.count = next;
			// now insert into the hash table
			InsertHashes(hashes, key_locations);

			entry += next;
		}
		pinned_handles.push_back(move(handle));
	}
	finalized = true;
}

unique_ptr<ScanStructure> JoinHashTable::Probe(DataChunk &keys) {
	assert(count > 0); // should be handled before
	assert(finalized);
	assert(!keys.sel_vector); // should be flattened before

	for (index_t i = 0; i < keys.column_count; i++) {
		if (null_values_are_equal[i]) {
			VectorOperations::FillNullMask(keys.data[i]);
		}
	}

	// scan structure
	auto ss = make_unique<ScanStructure>(*this);
	// first hash all the keys to do the lookup
	Vector hashes(TypeId::HASH, true, false);
	Hash(keys, hashes);

	// use bitmask to get index in array
	ApplyBitmask(hashes);

	// FIXME: optimize for constant key vector
	hashes.Normalify();

	// now create the initial pointers from the hashes
	auto ptrs = (data_ptr_t *)ss->pointers.GetData();
	auto indices = (uint64_t *)hashes.GetData();
	auto hashed_pointers = (data_ptr_t *)hash_map->node->buffer;
	for (index_t i = 0; i < hashes.count; i++) {
		auto index = indices[i];
		ptrs[i] = hashed_pointers[index];
	}
	ss->pointers.count = hashes.count;

	switch (join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::LEFT:
	case JoinType::SINGLE:
	case JoinType::MARK:
		// initialize all tuples with found_match to false
		memset(ss->found_match, 0, sizeof(ss->found_match));
		break;
	default:
		break;
	}

	switch (join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::LEFT:
	case JoinType::SINGLE:
	case JoinType::MARK:
	case JoinType::INNER: {
		// create the selection vector linking to only non-empty entries
		index_t count = 0;
		for (index_t i = 0; i < ss->pointers.count; i++) {
			if (ptrs[i]) {
				ss->sel_vector[count++] = i;
			}
		}
		ss->pointers.sel_vector = ss->sel_vector;
		ss->pointers.count = count;
		break;
	}
	default:
		throw NotImplementedException("Unimplemented join type for hash join");
	}

	return ss;
}

ScanStructure::ScanStructure(JoinHashTable &ht) : ht(ht), finished(false) {
	pointers.Initialize(TypeId::POINTER, false);
	build_pointer_vector.Initialize(TypeId::POINTER, false);
	pointers.sel_vector = this->sel_vector;
}

void ScanStructure::Next(DataChunk &keys, DataChunk &left, DataChunk &result) {
	assert(!left.sel_vector && !keys.sel_vector); // should be flattened before
	if (finished) {
		return;
	}

	switch (ht.join_type) {
	case JoinType::INNER:
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

index_t ScanStructure::ResolvePredicates(DataChunk &keys, sel_t comparison_result[]) {
	Vector current_pointers;
	current_pointers.Reference(pointers);

	index_t comparison_count;
	for (index_t i = 0; i < ht.predicates.size(); i++) {
		// gather the data from the pointers
		Vector ht_data(keys.data[i].type, true, false);
		ht_data.sel_vector = current_pointers.sel_vector;
		ht_data.count = current_pointers.count;
		// we don't check for NULL values in the keys because either
		// (1) NULL values will have been filtered out before (null_values_are_equal = false) or
		// (2) we want NULL=NULL to be true (null_values_are_equal = true)
		VectorOperations::Gather::Set(current_pointers, ht_data, false);

		// set the selection vector
		assert(!keys.data[i].sel_vector);
		index_t old_count = keys.data[i].count;

		keys.data[i].sel_vector = ht_data.sel_vector;
		keys.data[i].count = ht_data.count;

		// perform the comparison expression
		switch (ht.predicates[i]) {
		case ExpressionType::COMPARE_EQUAL:
			comparison_count = VectorOperations::SelectEquals(keys.data[i], ht_data, comparison_result);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			comparison_count = VectorOperations::SelectGreaterThan(keys.data[i], ht_data, comparison_result);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			comparison_count = VectorOperations::SelectGreaterThanEquals(keys.data[i], ht_data, comparison_result);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			comparison_count = VectorOperations::SelectLessThan(keys.data[i], ht_data, comparison_result);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			comparison_count = VectorOperations::SelectLessThanEquals(keys.data[i], ht_data, comparison_result);
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			comparison_count = VectorOperations::SelectNotEquals(keys.data[i], ht_data, comparison_result);
			break;
		default:
			throw NotImplementedException("Unimplemented comparison type for join");
		}
		// reset the selection vector
		keys.data[i].sel_vector = nullptr;
		keys.data[i].count = old_count;

		if (comparison_count == 0) {
			// no matches remaining, skip any remaining comparisons
			index_t increment = 0;
			for (index_t j = i; j < ht.predicates.size(); j++) {
				increment += GetTypeIdSize(keys.data[j].type);
			}
			VectorOperations::AddInPlace(pointers, increment);
			return 0;
		}
		if (i + 1 < ht.predicates.size()) {
			// more predicates remaining: update the selection vector to avoid unnecessary comparisons
			current_pointers.sel_vector = comparison_result;
			current_pointers.count = comparison_count;
		}
		// move all the pointers to the next element
		VectorOperations::AddInPlace(pointers, GetTypeIdSize(keys.data[i].type));
	}
	return comparison_count;
}

void ScanStructure::ResolvePredicates(DataChunk &keys, Vector &final_result) {
	// initialize result to false
	final_result.sel_vector = pointers.sel_vector;
	final_result.count = pointers.count;
	auto result_data = (bool *)final_result.GetData();
	VectorOperations::Exec(final_result, [&](index_t i, index_t k) { result_data[i] = false; });

	// now resolve the predicates with the keys
	sel_t matching_tuples[STANDARD_VECTOR_SIZE];
	index_t match_count = ResolvePredicates(keys, matching_tuples);

	// finished with all comparisons, mark the matching tuples
	for (index_t i = 0; i < match_count; i++) {
		result_data[matching_tuples[i]] = true;
	}
}

index_t ScanStructure::ScanInnerJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	sel_t comparison_result[STANDARD_VECTOR_SIZE];
	index_t result_count = 0;
	do {
		auto build_pointers = (data_ptr_t *)build_pointer_vector.GetData();

		// resolve the predicates for all the pointers
		result_count = ResolvePredicates(keys, comparison_result);

		auto ptrs = (data_ptr_t *)pointers.GetData();
		// after doing all the comparisons we loop to find all the actual matches (if any)
		for (index_t i = 0; i < result_count; i++) {
			auto index = comparison_result[i];
			found_match[index] = true;
			result.owned_sel_vector[i] = index;
			build_pointers[i] = ptrs[index];
		}

		// finally we chase the pointers for the next iteration
		index_t new_count = 0;
		VectorOperations::Exec(pointers, [&](index_t index, index_t k) {
			auto prev_pointer = (data_ptr_t *)(ptrs[index] + ht.build_size);
			ptrs[index] = *prev_pointer;
			if (ptrs[index]) {
				// if there is a next pointer, we keep this entry
				// otherwise the entry is removed from the sel_vector
				sel_vector[new_count++] = index;
			}
		});
		pointers.count = new_count;
	} while (pointers.count > 0 && result_count == 0);
	return result_count;
}

void ScanStructure::NextInnerJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	assert(result.column_count == left.column_count + ht.build_types.size());
	if (pointers.count == 0) {
		// no pointers left to chase
		return;
	}

	index_t result_count = ScanInnerJoin(keys, left, result);
	if (result_count > 0) {
		// matches were found
		// construct the result
		result.sel_vector = result.owned_sel_vector;
		build_pointer_vector.count = result_count;

		// reference the columns of the left side from the result
		for (index_t i = 0; i < left.column_count; i++) {
			result.data[i].Reference(left.data[i]);
			result.data[i].sel_vector = result.sel_vector;
			result.data[i].count = result_count;
		}
		// apply the selection vector
		// now fetch the right side data from the HT
		for (index_t i = 0; i < ht.build_types.size(); i++) {
			auto &vector = result.data[left.column_count + i];
			vector.sel_vector = result.sel_vector;
			vector.count = result_count;
			VectorOperations::Gather::Set(build_pointer_vector, vector);
			VectorOperations::AddInPlace(build_pointer_vector, GetTypeIdSize(ht.build_types[i]));
		}
	}
}

void ScanStructure::ScanKeyMatches(DataChunk &keys) {
	// the semi-join, anti-join and mark-join we handle a differently from the inner join
	// since there can be at most STANDARD_VECTOR_SIZE results
	// we handle the entire chunk in one call to Next().
	// for every pointer, we keep chasing pointers and doing comparisons.
	// this results in a boolean array indicating whether or not the tuple has a match
	Vector comparison_result(TypeId::BOOL);
	while (pointers.count > 0) {
		// resolve the predicates for the current set of pointers
		ResolvePredicates(keys, comparison_result);

		// after doing all the comparisons we loop to find all the matches
		auto ptrs = (data_ptr_t *)pointers.GetData();
		index_t new_count = 0;
		VectorOperations::ExecType<bool>(comparison_result, [&](bool match, index_t index, index_t k) {
			if (match) {
				// found a match, set the entry to true
				// after this we no longer need to check this entry
				found_match[index] = true;
			} else {
				// did not find a match, keep on looking for this entry
				// first check if there is a next entry
				auto prev_pointer = (data_ptr_t *)(ptrs[index] + ht.build_size);
				ptrs[index] = *prev_pointer;
				if (ptrs[index]) {
					// if there is a next pointer, we keep this entry
					sel_vector[new_count++] = index;
				}
			}
		});
		pointers.count = new_count;
	}
}

template <bool MATCH> void ScanStructure::NextSemiOrAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	assert(left.column_count == result.column_count);
	assert(keys.size() == left.size());
	// create the selection vector from the matches that were found
	index_t result_count = 0;
	for (index_t i = 0; i < keys.size(); i++) {
		if (found_match[i] == MATCH) {
			// part of the result
			result.owned_sel_vector[result_count++] = i;
		}
	}
	// construct the final result
	if (result_count > 0) {
		// we only return the columns on the left side
		// project them using the result selection vector
		result.sel_vector = result.owned_sel_vector;
		// reference the columns of the left side from the result
		for (index_t i = 0; i < left.column_count; i++) {
			result.data[i].Reference(left.data[i]);
			result.data[i].sel_vector = result.sel_vector;
			result.data[i].count = result_count;
		}
	} else {
		assert(result.size() == 0);
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

namespace duckdb {
void ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &child, DataChunk &result, bool found_match[],
                             bool right_has_null) {
	// for the initial set of columns we just reference the left side
	for (index_t i = 0; i < child.column_count; i++) {
		result.data[i].Reference(child.data[i]);
	}
	// create the result matching vector
	auto &result_vector = result.data[child.column_count];
	result_vector.count = child.size();
	// first we set the NULL values from the join keys
	// if there is any NULL in the keys, the result is NULL
	if (join_keys.column_count > 0) {
		result_vector.nullmask = join_keys.data[0].nullmask;
		for (index_t i = 1; i < join_keys.column_count; i++) {
			result_vector.nullmask |= join_keys.data[i].nullmask;
		}
	}
	// now set the remaining entries to either true or false based on whether a match was found
	auto bool_result = (bool *)result_vector.GetData();
	for (index_t i = 0; i < result_vector.count; i++) {
		bool_result[i] = found_match[i];
	}
	// if the right side contains NULL values, the result of any FALSE becomes NULL
	if (right_has_null) {
		for (index_t i = 0; i < result_vector.count; i++) {
			if (!bool_result[i]) {
				result_vector.nullmask[i] = true;
			}
		}
	}
}
} // namespace duckdb

void ScanStructure::NextMarkJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	assert(result.column_count == left.column_count + 1);
	assert(result.data[left.column_count].type == TypeId::BOOL);
	assert(!left.sel_vector);
	// this method should only be called for a non-empty HT
	assert(ht.count > 0);

	ScanKeyMatches(keys);
	if (ht.correlated_mark_join_info.correlated_types.size() == 0) {
		ConstructMarkJoinResult(keys, left, result, found_match, ht.has_null);
	} else {
		auto &info = ht.correlated_mark_join_info;
		// there are correlated columns
		// first we fetch the counts from the aggregate hashtable corresponding to these entries
		assert(keys.column_count == info.group_chunk.column_count + 1);
		for (index_t i = 0; i < info.group_chunk.column_count; i++) {
			info.group_chunk.data[i].Reference(keys.data[i]);
		}
		info.group_chunk.sel_vector = keys.sel_vector;
		info.correlated_counts->FetchAggregates(info.group_chunk, info.result_chunk);
		assert(!info.result_chunk.sel_vector);

		// for the initial set of columns we just reference the left side
		for (index_t i = 0; i < left.column_count; i++) {
			result.data[i].Reference(left.data[i]);
		}
		// create the result matching vector
		auto &result_vector = result.data[left.column_count];
		result_vector.count = result.data[0].count;
		// first set the nullmask based on whether or not there were NULL values in the join key
		result_vector.nullmask = keys.data[keys.column_count - 1].nullmask;

		auto bool_result = (bool *)result_vector.GetData();
		auto count_star = (int64_t *)info.result_chunk.data[0].GetData();
		auto count = (int64_t *)info.result_chunk.data[1].GetData();
		// set the entries to either true or false based on whether a match was found
		for (index_t i = 0; i < result_vector.count; i++) {
			assert(count_star[i] >= count[i]);
			bool_result[i] = found_match[i];
			if (!bool_result[i] && count_star[i] > count[i]) {
				// RHS has NULL value and result is false:, set to null
				result_vector.nullmask[i] = true;
			}
			if (count_star[i] == 0) {
				// count == 0, set nullmask to false (we know the result is false now)
				result_vector.nullmask[i] = false;
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
		index_t remaining_count = 0;
		for (index_t i = 0; i < left.size(); i++) {
			if (!found_match[i]) {
				result.owned_sel_vector[remaining_count++] = i;
			}
		}
		if (remaining_count > 0) {
			// have remaining tuples
			// first set the left side
			result.sel_vector = result.owned_sel_vector;
			index_t i = 0;
			for (; i < left.column_count; i++) {
				result.data[i].Reference(left.data[i]);
				result.data[i].sel_vector = result.sel_vector;
				result.data[i].count = remaining_count;
			}
			// now set the right side to NULL
			for (; i < result.column_count; i++) {
				result.data[i].nullmask.set();
				result.data[i].sel_vector = result.sel_vector;
				result.data[i].count = remaining_count;
			}
		}
		finished = true;
	}
}

void ScanStructure::NextSingleJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// single join
	// this join is similar to the semi join except that
	// (1) we actually return data from the RHS and
	// (2) we return NULL for that data if there is no match
	Vector comparison_result(TypeId::BOOL);

	auto build_pointers = (data_ptr_t *)build_pointer_vector.GetData();
	index_t result_count = 0;
	sel_t result_sel_vector[STANDARD_VECTOR_SIZE];
	while (pointers.count > 0) {
		// resolve the predicates for all the pointers
		ResolvePredicates(keys, comparison_result);

		auto ptrs = (data_ptr_t *)pointers.GetData();
		// after doing all the comparisons we loop to find all the actual matches
		index_t new_count = 0;
		VectorOperations::ExecType<bool>(comparison_result, [&](bool match, index_t index, index_t k) {
			if (match) {
				// found a match for this index
				// set the build_pointers to this position
				found_match[index] = true;
				build_pointers[result_count] = ptrs[index];
				result_sel_vector[result_count] = index;
				result_count++;
			} else {
				auto prev_pointer = (data_ptr_t *)(ptrs[index] + ht.build_size);
				ptrs[index] = *prev_pointer;
				if (ptrs[index]) {
					// if there is a next pointer, and we have not found a match yet, we keep this entry
					pointers.sel_vector[new_count++] = index;
				}
			}
		});
		pointers.count = new_count;
	}

	// now we construct the final result
	build_pointer_vector.count = result_count;
	// reference the columns of the left side from the result
	assert(left.column_count > 0);
	for (index_t i = 0; i < left.column_count; i++) {
		result.data[i].Reference(left.data[i]);
	}
	// now fetch the data from the RHS
	for (index_t i = 0; i < ht.build_types.size(); i++) {
		auto &vector = result.data[left.column_count + i];
		// set NULL entries for every entry that was not found
		vector.nullmask.set();
		for (index_t j = 0; j < result_count; j++) {
			vector.nullmask[result_sel_vector[j]] = false;
		}
		// fetch the data from the HT for tuples that found a match
		vector.sel_vector = result_sel_vector;
		vector.count = result_count;
		VectorOperations::Gather::Set(build_pointer_vector, vector);
		VectorOperations::AddInPlace(build_pointer_vector, GetTypeIdSize(ht.build_types[i]));
		// now we fill in NULL values in the remaining entries
		vector.count = result.size();
		vector.sel_vector = result.sel_vector;
	}
	// like the SEMI, ANTI and MARK join types, the SINGLE join only ever does one pass over the HT per input chunk
	finished = true;
}
