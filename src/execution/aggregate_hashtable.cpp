
#include "execution/aggregate_hashtable.hpp"
#include "common/exception.hpp"
#include "common/types/vector_operations.hpp"

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
	// [FLAG][GROUPS][PAYLOAD][COUNT]
	// [FLAG] is the state of the tuple in memory
	// [GROUPS] is the groups
	// [PAYLOAD] is the payload (i.e. the aggregates)
	// [COUNT] is an 8-byte count for each element
	tuple_size = FLAG_SIZE + (group_width + payload_width + sizeof(int64_t));
	Resize(initial_capacity);
}

SuperLargeHashTable::~SuperLargeHashTable() {}

void SuperLargeHashTable::Resize(size_t size) {
	if (size <= capacity) {
		throw NotImplementedException("Cannot downsize!");
	}

	if (entries > 0) {
		throw NotImplementedException(
		    "Resizing a filled HT not implemented yet!");
	} else {
		data = new uint8_t[size * tuple_size];
		owned_data = unique_ptr<uint8_t[]>(data);
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
	VectorOperations::Hash(groups.data[0], hashes);
	for (size_t i = 1; i < groups.column_count; i++) {
		VectorOperations::CombineHash(hashes, groups.data[i], hashes);
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
	sel_t new_entries[addresses.count];
	sel_t updated_entries[addresses.count];

	size_t new_count = 0, updated_count = 0;

	void **ptr = (void **)addresses.data;
	for (size_t i = 0; i < addresses.count; i++) {
		// first copy the group data for this tuple into a local space
		size_t group_position = 0;
		for (size_t grp = 0; grp < groups.column_count; grp++) {
			size_t data_size = GetTypeIdSize(groups.data[grp].type);
			size_t group_entry = groups.data[grp].sel_vector
			                         ? groups.data[grp].sel_vector[i]
			                         : i;
			if (groups.data[grp].type == TypeId::VARCHAR) {
				// inline strings
				const char *str =
				    ((const char **)groups.data[grp].data)[group_entry];
				// strings > 8 characters we need to store a pointer and compare
				// pointers
				// FIXME: use statistics to figure this out per column
				assert(strlen(str) <= 7);
				memset(group_data + group_position, 0, data_size);
				strcpy((char *)group_data + group_position, str);
			} else {
				memcpy(group_data + group_position,
				       groups.data[grp].data + data_size * group_entry,
				       data_size);
			}
			group_position += data_size;
		}

		// place this tuple in the hash table
		uint8_t *entry = (uint8_t *)ptr[i];

		size_t chain = 0;
		do {
			// check if the group is the actual group for this tuple or if it is
			// empty otherwise we have to do linear probing
			if (*entry == EMPTY_CELL) {
				// cell is empty; zero initialize the aggregates and add an
				// entry
				*entry = FULL_CELL;
				memcpy(entry + FLAG_SIZE, group_data, group_width);
				memset(entry + group_width + FLAG_SIZE, 0,
				       payload_width + sizeof(uint64_t));
				new_entries[new_count++] = i;
				entries++;
				break;
			}
			if (memcmp(group_data, entry + FLAG_SIZE, group_width) == 0) {
				// cell has the current group, just point to this cell
				updated_entries[updated_count++] = i;
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

		// resize at 50% capacity
		if (entries > capacity / 2) {
			Resize(capacity * 2);
		}
	}

	// now every cell has an entry
	// update the aggregates
	Vector one(Value::BIGINT(1));
	size_t j = 0;
	for (size_t i = 0; i < aggregate_types.size(); i++) {
		if (aggregate_types[i] == ExpressionType::AGGREGATE_COUNT_STAR) {
			continue;
		}
		if (new_count > 0) {
			// for any entries for which a new entry was created, set the
			// initial value
			// the payload might already have a selection vector
			// first store a reference to the old selection vector
			auto old_owned_sel_vector = move(payload.data[j].owned_sel_vector);
			auto old_sel_vector = payload.data[j].sel_vector;
			// now set the selection vector for the entries
			payload.data[j].SetSelVector(new_entries, new_count);
			addresses.sel_vector = new_entries;
			payload.data[j].count = addresses.count = new_count;
			switch (aggregate_types[i]) {
			case ExpressionType::AGGREGATE_COUNT:
				VectorOperations::Scatter::SetCount(payload.data[j], addresses);
				break;
			case ExpressionType::AGGREGATE_SUM:
			case ExpressionType::AGGREGATE_MIN:
			case ExpressionType::AGGREGATE_MAX:
				VectorOperations::Scatter::Set(payload.data[j], addresses);
				break;
			default:
				throw NotImplementedException("Unimplemented aggregate type!");
			}
			// restore the old selection vector
			payload.data[j].owned_sel_vector = move(old_owned_sel_vector);
			payload.data[j].sel_vector = old_sel_vector;
			payload.data[j].count = payload.count;
		}
		if (updated_count > 0) {
			// for any entries for which a group was found, update the aggregate
			// store the old selection vector
			auto old_owned_sel_vector = move(payload.data[j].owned_sel_vector);
			auto old_sel_vector = payload.data[j].sel_vector;
			payload.data[j].SetSelVector(updated_entries, updated_count);
			addresses.sel_vector = updated_entries;
			payload.data[j].count = addresses.count = updated_count;

			switch (aggregate_types[i]) {
			case ExpressionType::AGGREGATE_COUNT:
				VectorOperations::Scatter::AddOne(payload.data[j], addresses);
				break;
			case ExpressionType::AGGREGATE_SUM:
				// addition
				VectorOperations::Scatter::Add(payload.data[j], addresses);
				break;
			case ExpressionType::AGGREGATE_MIN:
				// min
				VectorOperations::Scatter::Min(payload.data[j], addresses);
				break;
			case ExpressionType::AGGREGATE_MAX:
				// max
				VectorOperations::Scatter::Max(payload.data[j], addresses);
				break;
			default:
				throw NotImplementedException("Unimplemented aggregate type!");
			}

			// restore the old selection vector
			payload.data[j].owned_sel_vector = move(old_owned_sel_vector);
			payload.data[j].sel_vector = old_sel_vector;
			payload.data[j].count = payload.count;
		}
		// move to the next aggregate chunk
		addresses.sel_vector = nullptr;
		addresses.count = groups.count;
		VectorOperations::Add(addresses, GetTypeIdSize(payload.data[j].type),
		                      addresses);
		j++;
	}
	// update the counts in each bucket
	VectorOperations::Scatter::Add(one, addresses);
}

void SuperLargeHashTable::Scan(size_t &scan_position, DataChunk &groups,
                               DataChunk &result) {
	result.Reset();

	uint8_t *ptr;
	uint8_t *start = data + scan_position * tuple_size;
	uint8_t *end = data + capacity * tuple_size;
	if (start >= end)
		return;

	Vector addresses(TypeId::POINTER, result.maximum_size);
	void **data_pointers = (void **)addresses.data;

	// scan the table for full cells starting from the scan position
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
	// fetch the group columns
	for (size_t i = 0; i < groups.column_count; i++) {
		auto &column = groups.data[i];
		column.count = entry;
		if (column.type == TypeId::VARCHAR) {
			const char **ptr = (const char **)addresses.data;
			for (size_t i = 0; i < entry; i++) {
				// fetch the string
				column.SetValue(i, Value(string(ptr[i])));
			}
		} else {
			VectorOperations::Gather::Set(addresses, column);
		}
		VectorOperations::Add(addresses, GetTypeIdSize(column.type), addresses);
	}

	size_t current_bytes = 0;
	for (size_t i = 0; i < aggregate_types.size(); i++) {
		auto &target = result.data[i];
		target.count = entry;

		if (aggregate_types[i] == ExpressionType::AGGREGATE_COUNT_STAR) {
			// we fetch the total counts later because they are stored at the
			// end
			continue;
		}
		VectorOperations::Gather::Set(addresses, target);
		VectorOperations::Add(addresses, GetTypeIdSize(target.type), addresses);
		current_bytes += GetTypeIdSize(target.type);
	}
	for (size_t i = 0; i < aggregate_types.size(); i++) {
		// now we can fetch the counts
		if (aggregate_types[i] == ExpressionType::AGGREGATE_COUNT_STAR) {
			auto &target = result.data[i];
			target.count = entry;
			VectorOperations::Gather::Set(addresses, target);
		}
	}
	groups.count = entry;
	result.count = entry;
	scan_position = ptr - start;
}
