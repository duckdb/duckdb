
#include "execution/aggregate_hashtable.hpp"
#include "common/exception.hpp"
#include "execution/vector/vector_operations.hpp"

using namespace duckdb;
using namespace std;

SuperLargeHashTable::SuperLargeHashTable(size_t initial_capacity,
                                         size_t group_width,
                                         size_t payload_width,
                                         vector<ExpressionType> aggregate_types,
                                         bool parallel)
    : entries(0), capacity(0), data(nullptr), group_width(group_width),
      payload_width(payload_width), aggregate_types(aggregate_types),
      max_chain(0), parallel(parallel) {
	// HT tuple layout is as follows:
	// [LOCK][GROUPS][PAYLOAD][COUNT]
	// [LOCK] is only added for parallel hashtables, and is LOCK_SIZE bytes
	// [GROUPS] is the groups
	// [PAYLOAD] is the payload (i.e. the aggregates)
	// [COUNT] is an 8-byte count for each element
	tuple_size = FLAG_SIZE + (group_width + payload_width + sizeof(uint64_t));
	Resize(initial_capacity);
}

SuperLargeHashTable::~SuperLargeHashTable() {
	if (data) {
		delete[] data;
	}
}

void SuperLargeHashTable::Resize(size_t size) {
	if (size <= capacity) {
		throw NotImplementedException("Cannot downsize!");
	}

	if (entries > 0) {
		throw NotImplementedException(
		    "Resizing a filled HT not implemented yet!");
	} else {
		if (data) {
			delete[] data;
		}
		data = new uint8_t[size * tuple_size];
		for (size_t i = 0; i < size; i++) {
			data[i * tuple_size] = EMPTY_CELL;
		}

		capacity = size;
	}
}

void SuperLargeHashTable::AddChunk(DataChunk &groups, DataChunk &payload) {
	if (groups.count == 0) {
		return;
	}
	// first create a hash of all the values
	Vector hashes(TypeId::INTEGER, groups.count);
	VectorOperations::Hash(*groups.data[0], hashes);
	for (size_t i = 1; i < groups.column_count; i++) {
		VectorOperations::CombineHash(hashes, *groups.data[i], hashes);
	}

	// list of addresses for the tuples
	Vector addresses(TypeId::POINTER, groups.count);
	// first cast from the hash type to the address type
	VectorOperations::Cast(hashes, addresses);
	// now compute the entry in the table based on the hash using a modulo
	VectorOperations::Modulo(addresses, capacity, addresses);
	// get the physical address of the tuple
	// multiply the position by the tuple size and add the base address
	VectorOperations::Multiply(addresses, tuple_size, addresses);
	VectorOperations::Add(addresses, (uint64_t)data, addresses);

	if (parallel) {
		throw NotImplementedException("Parallel HT not implemented");
	}

	// now we actually access the base table
	uint8_t group_data[group_width];

	void **ptr = (void **)addresses.data;
	for (size_t i = 0; i < addresses.count; i++) {
		// first copy the group data for this tuple into a local space
		size_t group_position = 0;
		for (size_t grp = 0; grp < groups.column_count; grp++) {
			size_t data_size = GetTypeIdSize(groups.data[grp]->type);
			memcpy(group_data + group_position,
			       groups.data[grp]->data + data_size * i, data_size);
			group_position += data_size;
		}

		// place this tuple in the hash table
		uint8_t *entry = (uint8_t *)ptr[i];

		size_t chain = 0;
		do {
			// check if the group is the actual group for this tuple or if it is
			// empty otherwise we have to do linear probing
			if (*entry == EMPTY_CELL) {
				// cell is empty; ero initialize the aggregates and add an entry
				*entry = FULL_CELL;
				memcpy(entry + FLAG_SIZE, group_data, group_width);
				memset(entry + group_width + FLAG_SIZE, 0,
				       payload_width + sizeof(uint64_t));
				entries++;
				break;
			}
			if (memcmp(group_data, entry + FLAG_SIZE, group_width) == 0) {
				// cell has the current group, just point to this cell
				break;
			}

			// collision: move to then next location
			chain++;
			entry += tuple_size;
			if (entry > data + tuple_size * capacity) {
				entry = data;
			}
		} while (true);

		// update the address pointer with the final position
		ptr[i] = entry + FLAG_SIZE + group_width;
		max_chain = max(chain, max_chain);
	}

	// now every cell has an entry
	// update the aggregates
	size_t j = 0;
	for (size_t i = 0; i < aggregate_types.size(); i++) {
		switch (aggregate_types[i]) {
		case ExpressionType::AGGREGATE_COUNT_STAR:
		case ExpressionType::AGGREGATE_COUNT:
			continue;
		case ExpressionType::AGGREGATE_SUM:
		case ExpressionType::AGGREGATE_AVG:
			// addition
			VectorOperations::Scatter::Add(*payload.data[j], ptr);
			break;
		case ExpressionType::AGGREGATE_MIN:
			// min
			VectorOperations::Scatter::Min(*payload.data[j], ptr);
			break;
		case ExpressionType::AGGREGATE_MAX:
			// max
			VectorOperations::Scatter::Max(*payload.data[j], ptr);
			break;

		default:
			throw NotImplementedException("Unimplemented aggregate type!");
		}
		VectorOperations::Add(addresses, GetTypeIdSize(payload.data[j]->type),
		                      addresses);
		j++;
	}
	// update the counts in each bucket
	Vector one(Value::NumericValue(TypeId::POINTER, 1));
	VectorOperations::Scatter::Add(one, ptr, addresses.count);
}

<<<<<<< HEAD

void SuperLargeHashTable::Scan(size_t& scan_position, DataChunk& groups, DataChunk& result) {
=======
void SuperLargeHashTable::Scan(size_t &scan_position, DataChunk &result) {
>>>>>>> 11b50762f086ca0383deff85bed24012278ef8f9
	result.Reset();

	uint8_t *ptr;
	uint8_t *start = data + scan_position * tuple_size;
	uint8_t *end = data + capacity * tuple_size;
	if (start >= end)
		return;

	Vector addresses(TypeId::POINTER, result.maximum_size);
<<<<<<< HEAD
	void **data_pointers = (void**) addresses.data;

	// scan the table for full cells starting from the scan position
=======
	void **data_pointers = (void **)addresses.data;

>>>>>>> 11b50762f086ca0383deff85bed24012278ef8f9
	size_t entry = 0;
	for (ptr = start; ptr < end && entry < result.maximum_size;
	     ptr += tuple_size) {
		if (*ptr == FULL_CELL) {
			// found entry
			data_pointers[entry++] = ptr + FLAG_SIZE;
		}
	}
	if (entry == 0) {
		return;
	}
	addresses.count = entry;
<<<<<<< HEAD
	// fetch the group columns
	for(size_t i = 0; i < groups.column_count; i++) {
		auto column = groups.data[i].get();
		column->count = entry;
		VectorOperations::Gather::Set(data_pointers, *column);
		VectorOperations::Add(addresses, GetTypeIdSize(column->type), addresses);
	}
	for(size_t i = 0; i < aggregate_types.size(); i++) {
=======
	for (size_t i = 0; i < aggregate_types.size(); i++) {
>>>>>>> 11b50762f086ca0383deff85bed24012278ef8f9
		auto target = result.data[i].get();
		target->count = entry;

		if (aggregate_types[i] == ExpressionType::AGGREGATE_COUNT_STAR ||
<<<<<<< HEAD
			aggregate_types[i] == ExpressionType::AGGREGATE_COUNT) {
			// we fetch the counts later because they are stored at the end
=======
		    aggregate_types[i] == ExpressionType::AGGREGATE_COUNT) {
			// we fetch the counts later
>>>>>>> 11b50762f086ca0383deff85bed24012278ef8f9
			continue;
		}
		VectorOperations::Gather::Set(data_pointers, *target);
		if (aggregate_types[i] == ExpressionType::AGGREGATE_AVG) {
			throw NotImplementedException("Gather count and divide!");
			// VectorOperations::Divide()
		}
		VectorOperations::Add(addresses, GetTypeIdSize(target->type),
		                      addresses);
	}
	for (size_t i = 0; i < aggregate_types.size(); i++) {
		// now we can fetch the counts
		if (aggregate_types[i] == ExpressionType::AGGREGATE_COUNT_STAR ||
		    aggregate_types[i] == ExpressionType::AGGREGATE_COUNT) {
			auto target = result.data[i].get();
			target->count = entry;
			VectorOperations::Gather::Set(data_pointers, *target);
		}
	}
	groups.count = entry;
	result.count = entry;
	scan_position = ptr - start;
}
