
#include "execution/aggregate_hashtable.hpp"
#include "common/exception.hpp"
#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

SuperLargeHashTable::SuperLargeHashTable(size_t initial_capacity,
                                         vector<TypeId> group_types,
                                         vector<TypeId> payload_types,
                                         vector<ExpressionType> aggregate_types,
                                         bool parallel)
    : entries(0), capacity(0), data(nullptr), group_width(0), payload_width(0),
      group_types(group_types), payload_types(payload_types),
      aggregate_types(aggregate_types), max_chain(0), parallel(parallel) {
	// HT tuple layout is as follows:
	// [FLAG][NULLMASK][GROUPS][PAYLOAD][COUNT]
	// [FLAG] is the state of the tuple in memory
	// [GROUPS] is the groups
	// [PAYLOAD] is the payload (i.e. the aggregates)
	// [COUNT] is an 8-byte count for each element
	for (auto type : group_types) {
		group_width += GetTypeIdSize(type);
	}
	for (auto type : payload_types) {
		payload_width += GetTypeIdSize(type);
	}
	tuple_size = FLAG_SIZE + (group_width + payload_width + sizeof(int64_t));
	Resize(initial_capacity);
}

SuperLargeHashTable::~SuperLargeHashTable() {}

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
		do {
			groups.Reset();
			payload.Reset();
			this->Scan(position, groups, payload);
			new_table->AddChunk(groups, payload);
		} while (groups.count > 0);

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

//! Writes NullValue<T> value of a specific type to a memory address
static void SetNullValue(uint8_t *ptr, TypeId type) {
	switch (type) {
	case TypeId::TINYINT:
		*((int8_t *)ptr) = NullValue<int8_t>();
		break;
	case TypeId::SMALLINT:
		*((int16_t *)ptr) = NullValue<int16_t>();
		break;
	case TypeId::INTEGER:
		*((int32_t *)ptr) = NullValue<int32_t>();
		break;
	case TypeId::DATE:
		*((date_t *)ptr) = NullValue<date_t>();
		break;
	case TypeId::BIGINT:
		*((int64_t *)ptr) = NullValue<int64_t>();
		break;
	case TypeId::TIMESTAMP:
		*((timestamp_t *)ptr) = NullValue<timestamp_t>();
		break;
	case TypeId::DECIMAL:
		*((double *)ptr) = NullValue<double>();
		break;
	default:
		throw Exception("Non-integer type in HT initialization!");
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
	Vector hashes(TypeId::INTEGER, true);
	VectorOperations::Hash(groups.data[0], hashes);
	for (size_t i = 1; i < groups.column_count; i++) {
		VectorOperations::CombineHash(hashes, groups.data[i], hashes);
	}

	assert(hashes.sel_vector == groups.sel_vector);
	// list of addresses for the tuples
	Vector addresses(TypeId::POINTER, true);
	// first cast from the hash type to the address type
	VectorOperations::Cast(hashes, addresses);
	assert(addresses.sel_vector == groups.sel_vector);
	// now compute the entry in the table based on the hash using a modulo
	VectorOperations::Modulo(addresses, capacity, addresses);
	// get the physical address of the tuple
	// multiply the position by the tuple size and add the base address
	VectorOperations::Multiply(addresses, tuple_size, addresses);
	VectorOperations::Add(addresses, (uint64_t)data, addresses);

	assert(addresses.sel_vector == groups.sel_vector);
	if (parallel) {
		throw NotImplementedException("Parallel HT not implemented");
	}

	// now we actually access the base table
	uint8_t group_data[group_width];
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
				if (groups.data[grp].nullmask[group_entry]) {
					SetNullValue(group_data + group_position,
					             groups.data[grp].type);
				} else {
					memcpy(group_data + group_position,
					       groups.data[grp].data + data_size * group_entry,
					       data_size);
				}
			}
			group_position += data_size;
		}

		// place this tuple in the hash table
		size_t index = addresses.sel_vector ? addresses.sel_vector[i] : i;
		uint8_t *entry = (uint8_t *)ptr[index];

		size_t chain = 0;
		do {
			// check if the group is the actual group for this tuple or if it is
			// empty otherwise we have to do linear probing
			if (*entry == EMPTY_CELL) {
				// cell is empty; zero initialize the aggregates and add an
				// entry
				*entry = FULL_CELL;
				memcpy(entry + FLAG_SIZE, group_data, group_width);
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
				*((int64_t *)location) = 0;
				entries++;
				break;
			}
			if (memcmp(group_data, entry + FLAG_SIZE, group_width) == 0) {
				// cell has the current group, just point to this cell
				break;
			}

			// collision: move to the next location
			chain++;
			entry += tuple_size;
			if (entry > data + tuple_size * capacity) {
				entry = data;
			}
		} while (true);

		// update the address pointer with the final position
		ptr[index] = entry + FLAG_SIZE + group_width;
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
		default:
			throw NotImplementedException("Unimplemented aggregate type!");
		}
		// move to the next aggregate
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

	Vector addresses(TypeId::POINTER, true);
	void **data_pointers = (void **)addresses.data;

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
