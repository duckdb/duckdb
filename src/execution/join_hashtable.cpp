
#include "execution/join_hashtable.hpp"
#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

using ScanStructure = JoinHashTable::ScanStructure;

JoinHashTable::JoinHashTable(std::vector<TypeId> key_types,
                             std::vector<TypeId> build_types,
                             size_t initial_capacity, bool parallel)
    : key_serializer(key_types), build_serializer(build_types),
      key_types(key_types), build_types(build_types), key_size(0),
      build_size(0), tuple_size(0), capacity(0), count(0), parallel(parallel) {
	for (size_t i = 0; i < key_types.size(); i++) {
		key_size += GetTypeIdSize(key_types[i]);
	}
	for (size_t i = 0; i < build_types.size(); i++) {
		build_size += GetTypeIdSize(build_types[i]);
	}
	tuple_size = key_size + build_size;
	entry_size = tuple_size + sizeof(void *);
	Resize(initial_capacity);
}

void JoinHashTable::InsertHashes(Vector &hashes, uint8_t *key_locations[]) {
	assert(hashes.type == TypeId::POINTER);
	hashes.Flatten();

	// use modulo to get position in array (FIXME: can be done more efficiently)
	VectorOperations::ModuloInPlace(hashes, capacity);

	auto pointers = hashed_pointers.get();
	auto indices = (uint64_t*) hashes.data;
	// now fill in the entries
	for(size_t i = 0; i < hashes.count; i++) {
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
		keys.Initialize(key_types);

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
			key_serializer.Deserialize(entry_pointers, keys);

			// create the hash
			Vector hashes;
			keys.Hash(hashes);

			// re-insert the entries
			InsertHashes(hashes, key_locations);

			// move to the next node
			node = node->prev.get();
		}
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

	// get the locations of where to serialize the keys and payload columns
	uint8_t *key_locations[STANDARD_VECTOR_SIZE];
	uint8_t *tuple_locations[STANDARD_VECTOR_SIZE];
	auto node = make_unique<Node>(entry_size, keys.size());
	auto dataptr = node->data.get();
	for (size_t i = 0; i < keys.size(); i++) {
		// key is stored at the start
		key_locations[i] = dataptr;
		// after that the build-side tuple is stored
		tuple_locations[i] = dataptr + key_size;
		dataptr += entry_size;
	}
	node->count = keys.size();
	// serialize the values to these locations
	key_serializer.Serialize(keys, key_locations);
	build_serializer.Serialize(payload, tuple_locations);

	// hash the keys and obtain an entry in the list
	Vector hashes;
	keys.Hash(hashes);

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

	// finally move strings to the string heap
	keys.MoveStringsToHeap(string_heap);
	payload.MoveStringsToHeap(string_heap);
}

unique_ptr<ScanStructure> JoinHashTable::Probe(DataChunk &keys) {
	assert(!keys.sel_vector); // should be flattened before
	// scan structure
	auto ss = make_unique<ScanStructure>(*this);
	// first hash all the keys to do the lookup
	Vector hashes;
	keys.Hash(hashes);
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

	// create the selection vector linking to only non-empty entries
	size_t count = 0;
	for (size_t i = 0; i < ss->pointers.count; i++) {
		if (ptrs[i]) {
			ss->sel_vector[count++] = i;
		}
	}
	ss->pointers.count = count;

	// serialize the keys for later comparison purposes
	key_serializer.Serialize(keys, ss->serialized_keys);

	return ss;
}

ScanStructure::ScanStructure(JoinHashTable &ht) : ht(ht) {
	pointers.Initialize(TypeId::POINTER, false);
	build_pointer_vector.Initialize(TypeId::POINTER, false);
	pointers.sel_vector = this->sel_vector;
}

void ScanStructure::Next(DataChunk &left, DataChunk &result) {
	assert(!left.sel_vector); // should be flattened before
	assert(result.column_count == left.column_count + ht.build_types.size());
	if (pointers.count == 0) {
		// no pointers left to chase
		return;
	}

	size_t result_count;
	auto build_pointers = (uint8_t **)build_pointer_vector.data;
	do {
		auto ptrs = (uint8_t **)pointers.data;
		result_count = 0;
		// perform the actual matching at this location for every entry in the
		// chain
		size_t new_count = 0;
		for (size_t i = 0; i < pointers.count; i++) {
			auto index = sel_vector[i];
			auto pointer = ptrs[index];
			assert(pointer);
			// check if there is an actual match here
			if (ht.key_serializer.Compare(serialized_keys[index].data.get(),
			                              pointer) == 0) {
				// if there is, we have to add this entry to the result
				result.owned_sel_vector[result_count] = index;
				build_pointers[result_count] = pointer + ht.key_size;
				result_count++;
			}
			// follow the pointer to the next entry in the chain
			// check if there is a next pointer
			auto prev_pointer = (uint8_t **)(ptrs[index] + ht.tuple_size);
			ptrs[index] = *prev_pointer;
			if (ptrs[index]) {
				// if there is a next pointer, we keep this entry
				// otherwise the entry is removed from the sel_vector
				sel_vector[new_count++] = index;
			}
		}
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
			ht.build_serializer.DeserializeColumn(
			    build_pointer_vector, i, result.data[left.column_count + i]);
		}
	}
}
