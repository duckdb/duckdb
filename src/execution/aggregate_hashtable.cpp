
#include "execution/aggregate_hashtable.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/types/vector_operations.hpp"

#include <map>

using namespace duckdb;
using namespace std;

SuperLargeHashTable::SuperLargeHashTable(size_t initial_capacity,
                                         vector<TypeId> group_types,
                                         vector<TypeId> payload_types,
                                         vector<ExpressionType> aggregate_types,
                                         bool parallel)
    : group_serializer(group_types, false), aggregate_types(aggregate_types),
      group_types(group_types), payload_types(payload_types), payload_width(0),
      capacity(0), entries(0), data(nullptr), max_chain(0), parallel(parallel) {
	// HT tuple layout is as follows:
	// [FLAG][NULLMASK][GROUPS][PAYLOAD][COUNT]
	// [FLAG] is the state of the tuple in memory
	// [GROUPS] is the groups
	// [PAYLOAD] is the payload (i.e. the aggregates)
	// [COUNT] is an 8-byte count for each element
	for (auto type : payload_types) {
		payload_width += GetTypeIdSize(type);
	}
	tuple_size = FLAG_SIZE + (group_serializer.TupleSize() + payload_width +
	                          sizeof(int64_t));
	Resize(initial_capacity);
}

SuperLargeHashTable::~SuperLargeHashTable() {
}

void SuperLargeHashTable::Resize(size_t size) {
	if (size <= capacity) {
		throw Exception("Cannot downsize a hash table!");
	}

	if (entries > 0) {
		DataChunk groups;
		DataChunk payload;
		size_t position = 0;

		groups.Initialize(group_types, false);
		payload.Initialize(payload_types, false);

		auto new_table = make_unique<SuperLargeHashTable>(
		    size, group_types, payload_types, aggregate_types, parallel);

		while (true) {
			groups.Reset();
			payload.Reset();
			this->Scan(position, groups, payload);
			if (groups.count == 0) {
				break;
			}
			new_table->AddChunk(groups, payload);
		}

		assert(this->entries == new_table->entries);

		this->data = move(new_table->data);
		this->owned_data = move(new_table->owned_data);
		this->capacity = new_table->capacity;
		this->max_chain = new_table->max_chain;
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

	// resize at 50% capacity, also need to fit the entire vector
	if (entries > capacity / 2 || capacity - entries <= STANDARD_VECTOR_SIZE) {
		Resize(capacity * 2);
	}

	// we need to be able to fit at least one vector of data
	assert(capacity - entries > STANDARD_VECTOR_SIZE);

	// first create a hash of all the values
	Vector hashes(TypeId::INTEGER, true, false);
	VectorOperations::Hash(groups.data[0], hashes);
	for (size_t i = 1; i < groups.column_count; i++) {
		VectorOperations::CombineHash(hashes, groups.data[i], hashes);
	}

	assert(hashes.sel_vector == groups.sel_vector);
	// list of addresses for the tuples
	Vector addresses(TypeId::POINTER, true, false);
	auto data_pointers = (uint8_t **)addresses.data;
	// first cast from the hash type to the address type
	VectorOperations::Cast(hashes, addresses);
	assert(addresses.sel_vector == groups.sel_vector);
	// now compute the entry in the table based on the hash using a modulo
	// multiply the position by the tuple size and add the base address
	VectorOperations::ExecType<uint64_t>(
	    addresses, [&](uint64_t element, size_t i, size_t k) {
		    data_pointers[i] = data + ((element % capacity) * tuple_size);
	    });

	assert(addresses.sel_vector == groups.sel_vector);
	if (parallel) {
		throw NotImplementedException("Parallel HT not implemented");
	}

	auto group_width = group_serializer.TupleSize();

	// serialize the elements from the group to a tuple-wide format
	uint8_t group_data[STANDARD_VECTOR_SIZE][group_serializer.TupleSize()];
	uint8_t *group_elements[STANDARD_VECTOR_SIZE];
	for (size_t i = 0; i < groups.count; i++) {
		group_elements[i] = group_data[i];
	}
	group_serializer.Serialize(groups, group_elements);

	// now we actually access the base table
	for (size_t i = 0; i < addresses.count; i++) {
		// place this tuple in the hash table
		size_t index = addresses.sel_vector ? addresses.sel_vector[i] : i;
		auto entry = data_pointers[index];
		assert(entry >= data && entry < data + capacity * tuple_size);

		size_t chain = 0;
		do {
			// check if the group is the actual group for this tuple or if it is
			// empty otherwise we have to do linear probing
			if (*entry == EMPTY_CELL) {
				// cell is empty; zero initialize the aggregates and add an
				// entry
				*entry = FULL_CELL;
				memcpy(entry + FLAG_SIZE, group_elements[i], group_width);
				// initialize the aggragetes to the NULL value
				auto location = entry + group_width + FLAG_SIZE;
				for (size_t i = 0; i < payload_types.size(); i++) {
					// counts are zero initialized, all other aggregates NULL
					// initialized
					auto type = payload_types[i];
					if (aggregate_types[i] == ExpressionType::AGGREGATE_COUNT) {
						memset(location, 0, GetTypeIdSize(type));
					} else {
						SetNullValue(location, type);
					}
					location += GetTypeIdSize(type);
				}
				// set the count
				*((int64_t *)location) = 0;
				entries++;
				break;
			}
			if (group_serializer.Compare(group_elements[i],
			                             entry + FLAG_SIZE) == 0) {
				// cell has the current group, just point to this cell
				break;
			}

			// collision: move to the next location
			chain++;
			entry += tuple_size;
			if (entry >= data + tuple_size * capacity) {
				entry = data;
			}
		} while (true);

		// update the address pointer with the final position
		data_pointers[index] = entry + FLAG_SIZE + group_width;
		max_chain = max(chain, max_chain);
	}

	// now every cell has an entry
	// update the aggregates
	Vector one(Value::BIGINT(1));
	size_t j = 0;

	for (size_t i = 0; i < aggregate_types.size(); i++) {
		if (aggregate_types[i] == ExpressionType::AGGREGATE_COUNT_STAR) {
			continue;
		}
		assert(payload.column_count > j);
		// for any entries for which a group was found, update the aggregate
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
		case ExpressionType::AGGREGATE_FIRST:
			// first
			VectorOperations::Scatter::SetFirst(payload.data[j], addresses);
			break;
		default:
			throw NotImplementedException("Unimplemented aggregate type!");
		}
		// move to the next aggregate
		VectorOperations::AddInPlace(addresses,
		                             GetTypeIdSize(payload.data[j].type));
		j++;
	}
	// update the counts in each bucket
	VectorOperations::Scatter::Add(one, addresses);
}

void SuperLargeHashTable::Scan(size_t &scan_position, DataChunk &groups,
                               DataChunk &result) {
	result.Reset();

	uint8_t *ptr;
	uint8_t *start = data + scan_position;
	uint8_t *end = data + capacity * tuple_size;
	if (start >= end)
		return;

	Vector addresses(TypeId::POINTER, true, false);
	auto data_pointers = (uint8_t **)addresses.data;

	// scan the table for full cells starting from the scan position
	size_t entry = 0;
	for (ptr = start; ptr < end && entry < STANDARD_VECTOR_SIZE;
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
		VectorOperations::Gather::Set(addresses, column);
		VectorOperations::AddInPlace(addresses, GetTypeIdSize(column.type));
	}

	for (size_t i = 0; i < aggregate_types.size(); i++) {
		auto &target = result.data[i];
		target.count = entry;

		if (aggregate_types[i] == ExpressionType::AGGREGATE_COUNT_STAR) {
			// we fetch the total counts later because they are stored at the
			// end
			continue;
		}
		VectorOperations::Gather::Set(addresses, target);
		VectorOperations::AddInPlace(addresses, GetTypeIdSize(target.type));
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
	scan_position = ptr - data;
}
