#include "execution/join_hashtable.hpp"

#include "common/exception.hpp"
#include "common/types/static_vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

using ScanStructure = JoinHashTable::ScanStructure;

JoinHashTable::JoinHashTable(vector<JoinCondition> &conditions, vector<TypeId> build_types, JoinType type,
                             size_t initial_capacity, bool parallel)
    : build_serializer(build_types), build_types(build_types), equality_size(0), condition_size(0), build_size(0),
      entry_size(0), tuple_size(0), join_type(type), capacity(0), count(0), parallel(parallel) {
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

		condition_types.push_back(type);
		condition_size += type_size;
	}
	// at least one equality is necessary
	assert(equality_types.size() > 0);
	equality_serializer.Initialize(equality_types);
	condition_serializer.Initialize(condition_types);

	if (type == JoinType::ANTI || type == JoinType::SEMI) {
		// for ANTI and SEMI join, we only need to store the keys
		build_size = 0;
	} else {
		// otherwise we need to store the entire build side for reconstruction
		// purposes
		for (size_t i = 0; i < build_types.size(); i++) {
			build_size += GetTypeIdSize(build_types[i]);
		}
	}
	tuple_size = condition_size + build_size;
	entry_size = tuple_size + sizeof(void *);
	Resize(initial_capacity);
}

void JoinHashTable::InsertHashes(Vector &hashes, uint8_t *key_locations[]) {
	assert(hashes.type == TypeId::POINTER);
	hashes.Flatten();

	// use modulo to get position in array (FIXME: can be done more efficiently)
	VectorOperations::ModuloInPlace(hashes, capacity);

	auto pointers = hashed_pointers.get();
	auto indices = (uint64_t *)hashes.data;
	// now fill in the entries
	for (size_t i = 0; i < hashes.count; i++) {
		auto index = indices[i];
		// set prev in current key to the value (NOTE: this will be nullptr if
		// there is none)
		auto prev_pointer = (uint8_t **)(key_locations[i] + tuple_size);
		*prev_pointer = pointers[index];

		// set pointer to current tuple
		pointers[index] = key_locations[i];
	}
}

void JoinHashTable::Resize(size_t size) {
	if (size <= capacity) {
		throw Exception("Cannot downsize a hash table!");
	}
	capacity = size;

	hashed_pointers = unique_ptr<uint8_t *[]>(new uint8_t *[capacity]);
	memset(hashed_pointers.get(), 0, capacity * sizeof(uint8_t *));

	if (count > 0) {
		// we have entries, need to rehash the pointers
		// first reset all chain pointers to the nullptr
		// we could do this by actually following the chains in the
		// hashed_pointers as well might be more or less efficient depending on
		// length of chains?
		auto node = head.get();
		while (node) {
			// scan all the entries in this node
			auto entry_pointer = (uint8_t *)node->data.get() + tuple_size;
			for (size_t i = 0; i < node->count; i++) {
				// reset chain pointer
				auto prev_pointer = (uint8_t **)entry_pointer;
				*prev_pointer = nullptr;
				// move to next entry
				entry_pointer += entry_size;
			}
			node = node->prev.get();
		}

		// now rehash the entries
		DataChunk keys;
		keys.Initialize(equality_types);

		Vector entry_pointers(TypeId::POINTER, true, true);
		auto deserialize_locations = (uint8_t **)entry_pointers.data;
		uint8_t *key_locations[STANDARD_VECTOR_SIZE];

		node = head.get();
		while (node) {
			// scan all the entries in this node
			auto dataptr = node->data.get();
			for (size_t i = 0; i < node->count; i++) {
				// key is stored at the start
				key_locations[i] = deserialize_locations[i] = dataptr;
				// move to next entry
				dataptr += entry_size;
			}
			entry_pointers.count = node->count;

			// reconstruct the keys chunk from the stored entries
			// we only reconstruct the keys that are part of the equality
			// comparison as these are the ones that are used to compute the
			// hash
			equality_serializer.Deserialize(entry_pointers, keys);

			// create the hash
			StaticVector<uint64_t> hashes;
			keys.Hash(hashes);

			// re-insert the entries
			InsertHashes(hashes, key_locations);

			// move to the next node
			node = node->prev.get();
		}
	}
}

void JoinHashTable::Hash(DataChunk &keys, Vector &hashes) {
	VectorOperations::Hash(keys.data[0], hashes);
	for (size_t i = 1; i < equality_types.size(); i++) {
		VectorOperations::CombineHash(hashes, keys.data[i]);
	}
}

void JoinHashTable::Build(DataChunk &keys, DataChunk &payload) {
	assert(keys.size() == payload.size());
	// resize at 50% capacity, also need to fit the entire vector
	if (parallel) {
		parallel_lock.lock();
	}
	if (count + keys.size() > capacity / 2) {
		Resize(capacity * 2);
	}
	count += keys.size();
	if (parallel) {
		parallel_lock.unlock();
	}

	// move strings to the string heap
	keys.MoveStringsToHeap(string_heap);
	payload.MoveStringsToHeap(string_heap);

	// get the locations of where to serialize the keys and payload columns
	uint8_t *key_locations[STANDARD_VECTOR_SIZE];
	uint8_t *tuple_locations[STANDARD_VECTOR_SIZE];
	auto node = make_unique<Node>(entry_size, keys.size());
	auto dataptr = node->data.get();
	for (size_t i = 0; i < keys.size(); i++) {
		// key is stored at the start
		key_locations[i] = dataptr;
		// after that the build-side tuple is stored
		tuple_locations[i] = dataptr + condition_size;
		dataptr += entry_size;
	}
	node->count = keys.size();

	// serialize the values to these locations
	// we serialize all the condition variables here
	condition_serializer.Serialize(keys, key_locations);
	if (build_size > 0) {
		build_serializer.Serialize(payload, tuple_locations);
	}

	// hash the keys and obtain an entry in the list
	// note that we only hash the keys used in the equality comparison
	StaticVector<uint64_t> hashes;
	Hash(keys, hashes);

	if (parallel) {
		// obtain lock
		parallel_lock.lock();
	}
	InsertHashes(hashes, key_locations);
	// store the new node as the head
	node->prev = move(head);
	head = move(node);
	if (parallel) {
		parallel_lock.unlock();
	}
}

unique_ptr<ScanStructure> JoinHashTable::Probe(DataChunk &keys) {
	assert(!keys.sel_vector); // should be flattened before
	// scan structure
	auto ss = make_unique<ScanStructure>(*this);
	// first hash all the keys to do the lookup
	StaticVector<uint64_t> hashes;
	Hash(keys, hashes);

	// use modulo to get index in array
	VectorOperations::ModuloInPlace(hashes, capacity);

	// now create the initial pointers from the hashes
	auto ptrs = (uint8_t **)ss->pointers.data;
	auto indices = (uint64_t *)hashes.data;
	for (size_t i = 0; i < hashes.count; i++) {
		auto index = indices[i];
		ptrs[i] = hashed_pointers[index];
	}
	ss->pointers.count = hashes.count;

	switch (join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::LEFT:
		// initialize all tuples with found_match to false
		memset(ss->found_match, 0, sizeof(ss->found_match));
	case JoinType::INNER: {
		// create the selection vector linking to only non-empty entries
		size_t count = 0;
		for (size_t i = 0; i < ss->pointers.count; i++) {
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
	case JoinType::ANTI:
		NextAntiJoin(keys, left, result);
		break;
	case JoinType::LEFT:
		NextLeftJoin(keys, left, result);
		break;
	default:
		throw Exception("Unhandled join type in JoinHashTable");
	}
}

void ScanStructure::ResolvePredicates(DataChunk &keys, Vector &final_result) {
	Vector current_pointers;
	current_pointers.Reference(pointers);

	sel_t temporary_selection_vector[STANDARD_VECTOR_SIZE];

	auto old_sel_vector = current_pointers.sel_vector;
	auto old_count = current_pointers.count;

	for (size_t i = 0; i < ht.predicates.size(); i++) {
		// gather the data from the pointers
		Vector ht_data(keys.data[i].type, true, false);
		ht_data.sel_vector = current_pointers.sel_vector;
		ht_data.count = current_pointers.count;

		VectorOperations::Gather::Set(current_pointers, ht_data);

		// set the selection vector
		size_t old_count = keys.data[i].count;
		assert(!keys.data[i].sel_vector);
		keys.data[i].sel_vector = ht_data.sel_vector;
		keys.data[i].count = ht_data.count;

		// perform the comparison expression
		switch (ht.predicates[i]) {
		case ExpressionType::COMPARE_EQUAL:
			VectorOperations::Equals(keys.data[i], ht_data, final_result);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			VectorOperations::GreaterThan(keys.data[i], ht_data, final_result);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			VectorOperations::GreaterThanEquals(keys.data[i], ht_data, final_result);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			VectorOperations::LessThan(keys.data[i], ht_data, final_result);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			VectorOperations::LessThanEquals(keys.data[i], ht_data, final_result);
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			VectorOperations::NotEquals(keys.data[i], ht_data, final_result);
			break;
		default:
			throw NotImplementedException("Unimplemented comparison type for join");
		}
		// reset the selection vector
		keys.data[i].sel_vector = nullptr;
		keys.data[i].count = old_count;

		// now based on the result recreate the selection vector for the next
		// step so we can skip unnecessary comparisons in the next phase
		if (i != ht.predicates.size() - 1) {
			size_t new_count = 0;
			VectorOperations::ExecType<bool>(final_result, [&](bool match, size_t index, size_t k) {
				if (match) {
					temporary_selection_vector[new_count++] = index;
				}
			});
			current_pointers.sel_vector = temporary_selection_vector;
			current_pointers.count = new_count;
		}
		// move all the pointers to the next element
		VectorOperations::AddInPlace(pointers, GetTypeIdSize(keys.data[i].type));
	}
	final_result.sel_vector = old_sel_vector;
	final_result.count = old_count;
}

void ScanStructure::NextInnerJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	assert(result.column_count == left.column_count + ht.build_types.size());
	if (pointers.count == 0) {
		// no pointers left to chase
		return;
	}

	StaticVector<bool> comparison_result;

	size_t result_count = 0;
	do {
		auto build_pointers = (uint8_t **)build_pointer_vector.data;

		// resolve the predicates for all the pointers
		ResolvePredicates(keys, comparison_result);

		auto ptrs = (uint8_t **)pointers.data;
		// after doing all the comparisons we loop to find all the actual
		// matches
		result_count = 0;
		VectorOperations::ExecType<bool>(comparison_result, [&](bool match, size_t index, size_t k) {
			if (match) {
				found_match[index] = true;
				result.owned_sel_vector[result_count] = index;
				build_pointers[result_count] = ptrs[index];
				result_count++;
			}
		});

		// finally we chase the pointers for the next iteration
		size_t new_count = 0;
		VectorOperations::Exec(pointers, [&](size_t index, size_t k) {
			auto prev_pointer = (uint8_t **)(ptrs[index] + ht.build_size);
			ptrs[index] = *prev_pointer;
			if (ptrs[index]) {
				// if there is a next pointer, we keep this entry
				// otherwise the entry is removed from the sel_vector
				sel_vector[new_count++] = index;
			}
		});
		pointers.count = new_count;
	} while (pointers.count > 0 && result_count == 0);

	if (result_count > 0) {
		// matches were found
		// construct the result
		result.sel_vector = result.owned_sel_vector;
		build_pointer_vector.count = result_count;

		// reference the columns of the left side from the result
		for (size_t i = 0; i < left.column_count; i++) {
			result.data[i].Reference(left.data[i]);
			result.data[i].sel_vector = result.sel_vector;
			result.data[i].count = result_count;
		}
		// apply the selection vector
		// now fetch the right side data from the HT
		for (size_t i = 0; i < ht.build_types.size(); i++) {
			auto &vector = result.data[left.column_count + i];
			vector.sel_vector = result.sel_vector;
			vector.count = result_count;
			ht.build_serializer.DeserializeColumn(build_pointer_vector, i, result.data[left.column_count + i]);
		}
	}
}

//! Implementation for semi (MATCH=true) or anti (MATCH=false) joins
template <bool MATCH> void ScanStructure::NextSemiOrAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	assert(left.column_count == result.column_count);
	// the semi-join and anti-join we handle a differently from the inner join
	// since there can be at most STANDARD_VECTOR_SIZE results
	// we handle the entire chunk in one call to Next().
	// for every pointer, we keep chasing pointers and doing comparisons.
	// if a matching tuple is found, we keep (semi) or discard (anti) the entry
	// if we reach the end of the chain without finding a match
	StaticVector<bool> comparison_result;
	while (pointers.count > 0) {
		// resolve the predicates for the current set of pointers
		ResolvePredicates(keys, comparison_result);

		// after doing all the comparisons we loop to find all the matches
		auto ptrs = (uint8_t **)pointers.data;
		size_t new_count = 0;
		VectorOperations::ExecType<bool>(comparison_result, [&](bool match, size_t index, size_t k) {
			if (match) {
				// found a match, set the entry to true
				// after this we no longer need to check this entry
				found_match[index] = true;
			} else {
				// did not find a match, keep on looking for this entry
				// first check if there is a next entry
				auto prev_pointer = (uint8_t **)(ptrs[index] + ht.build_size);
				ptrs[index] = *prev_pointer;
				if (ptrs[index]) {
					// if there is a next pointer, we keep this entry
					sel_vector[new_count++] = index;
				}
			}
		});
		pointers.count = new_count;
	}
	assert(keys.size() == left.size());
	// create the selection vector from the matches that were found
	size_t result_count = 0;
	for (size_t i = 0; i < keys.size(); i++) {
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
		for (size_t i = 0; i < left.column_count; i++) {
			result.data[i].Reference(left.data[i]);
			result.data[i].sel_vector = result.sel_vector;
			result.data[i].count = result_count;
		}
	} else {
		assert(result.size() == 0);
	}

	finished = true;
}

void ScanStructure::NextSemiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	NextSemiOrAntiJoin<true>(keys, left, result);
}

void ScanStructure::NextAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	NextSemiOrAntiJoin<false>(keys, left, result);
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
		size_t remaining_count = 0;
		for (size_t i = 0; i < left.size(); i++) {
			if (!found_match[i]) {
				result.owned_sel_vector[remaining_count++] = i;
			}
		}
		if (remaining_count > 0) {
			// have remaining tuples
			// first set the left side
			result.sel_vector = result.owned_sel_vector;
			size_t i = 0;
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
