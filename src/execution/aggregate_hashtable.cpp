#include "execution/aggregate_hashtable.hpp"

#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/types/static_vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <cmath>
#include <map>

using namespace duckdb;
using namespace std;

static index_t GetAggrPayloadSize(ExpressionType expr_type, TypeId return_type) {
	switch (expr_type) {
	case ExpressionType::AGGREGATE_COUNT:
	case ExpressionType::AGGREGATE_COUNT_DISTINCT:
	case ExpressionType::AGGREGATE_COUNT_STAR:
		// COUNT aggregates always use TypeId::BIGINT
		return GetTypeIdSize(TypeId::BIGINT);
	case ExpressionType::AGGREGATE_STDDEV_SAMP:
		// count running_mean running_dsquared
		return sizeof(uint64_t) + sizeof(double) + sizeof(double);
	default:
		return GetTypeIdSize(return_type);
	}
}

SuperLargeHashTable::SuperLargeHashTable(index_t initial_capacity, vector<TypeId> group_types,
                                         vector<TypeId> payload_types, vector<ExpressionType> aggregate_types,
                                         bool parallel)
    : group_serializer(group_types), aggregate_types(aggregate_types), group_types(group_types),
      payload_types(payload_types), payload_width(0), capacity(0), entries(0), data(nullptr),
      parallel(parallel) {
	// HT tuple layout is as follows:
	// [FLAG][NULLMASK][GROUPS][PAYLOAD][COUNT]
	// [FLAG] is the state of the tuple in memory
	// [GROUPS] is the groups
	// [PAYLOAD] is the payload (i.e. the aggregates)
	// [COUNT] is an 8-byte count for each element
	for (index_t i = 0; i < payload_types.size(); i++) {
		payload_width += GetAggrPayloadSize(aggregate_types[i], payload_types[i]);
	}
	empty_payload_data = unique_ptr<data_t[]>(new data_t[payload_width]);
	// initialize the aggregates to the NULL value
	auto pointer = empty_payload_data.get();
	for (index_t i = 0; i < payload_types.size(); i++) {
		// counts are zero initialized, all other aggregates NULL
		// initialized
		switch (aggregate_types[i]) {
		case ExpressionType::AGGREGATE_COUNT:
		case ExpressionType::AGGREGATE_COUNT_DISTINCT:
		case ExpressionType::AGGREGATE_COUNT_STAR:
		case ExpressionType::AGGREGATE_STDDEV_SAMP:
			memset(pointer, 0, GetAggrPayloadSize(aggregate_types[i], payload_types[i]));
			break;
		default:
			SetNullValue(pointer, payload_types[i]);
			break;
		}

		pointer += GetAggrPayloadSize(aggregate_types[i], payload_types[i]);
	}

	// FIXME: this always creates this vector, even if no distinct if present.
	// it likely does not matter.
	distinct_hashes.resize(aggregate_types.size());

	// create additional hash tables for distinct aggrs
	for (index_t i = 0; i < aggregate_types.size(); i++) {
		switch (aggregate_types[i]) {
		case ExpressionType::AGGREGATE_COUNT_DISTINCT:
		case ExpressionType::AGGREGATE_SUM_DISTINCT: {
			// group types plus aggr return type
			vector<TypeId> distinct_group_types(group_types);
			vector<TypeId> distinct_payload_types;
			vector<ExpressionType> distinct_aggregate_types;
			distinct_group_types.push_back(payload_types[i]);
			distinct_hashes[i] = make_unique<SuperLargeHashTable>(initial_capacity, distinct_group_types,
			                                                      distinct_payload_types, distinct_aggregate_types);
			break;
		}
		default:
			// nothing
			break;
		}
	}

	tuple_size = FLAG_SIZE + (group_serializer.TupleSize() + payload_width);
	Resize(initial_capacity);
}

SuperLargeHashTable::~SuperLargeHashTable() {
}

void SuperLargeHashTable::Resize(index_t size) {
	if (size <= capacity) {
		throw Exception("Cannot downsize a hash table!");
	}
	// size needs to be a power of 2
	assert((size & (size - 1)) == 0);
	bitmask = size - 1;

	if (entries > 0) {
		auto new_table = make_unique<SuperLargeHashTable>(size, group_types, payload_types, aggregate_types, parallel);

		DataChunk groups;
		groups.Initialize(group_types, false);

		Vector addresses(TypeId::POINTER, true, false);
		auto data_pointers = (data_ptr_t *)addresses.data;

		data_ptr_t ptr = data;
		data_ptr_t end = data + capacity * tuple_size;

		assert(new_table->tuple_size == this->tuple_size);

		while (true) {
			groups.Reset();

			// scan the table for full cells starting from the scan position
			index_t entry = 0;
			for (; ptr < end && entry < STANDARD_VECTOR_SIZE; ptr += tuple_size) {
				if (*ptr == FULL_CELL) {
					// found entry
					data_pointers[entry++] = ptr + FLAG_SIZE;
				}
			}
			if (entry == 0) {
				break;
			}
			addresses.count = entry;
			// fetch the group columns
			for (index_t i = 0; i < groups.column_count; i++) {
				auto &column = groups.data[i];
				column.count = entry;
				VectorOperations::Gather::Set(addresses, column);
				VectorOperations::AddInPlace(addresses, GetTypeIdSize(column.type));
			}

			groups.Verify();
			assert(groups.size() == entry);
			StaticPointerVector new_addresses;
			StaticVector<bool> new_group_dummy;
			new_table->FindOrCreateGroups(groups, new_addresses, new_group_dummy);

			// NB: both address vectors already point to the payload start
			assert(addresses.type == new_addresses.type && addresses.type == TypeId::POINTER);
			assert(addresses.count == new_addresses.count);
			assert(addresses.sel_vector == new_addresses.sel_vector);

			VectorOperations::Exec(addresses, [&](index_t i, index_t k) {
				memcpy(((data_ptr_t *)new_addresses.data)[i], data_pointers[i], payload_width);
			});
		}

		assert(this->entries == new_table->entries);

		this->data = move(new_table->data);
		this->owned_data = move(new_table->owned_data);
		this->capacity = new_table->capacity;

	} else {
		data = new data_t[size * tuple_size];
		owned_data = unique_ptr<data_t[]>(data);
		for (index_t i = 0; i < size; i++) {
			data[i * tuple_size] = EMPTY_CELL;
		}

		capacity = size;
	}

	endptr = data + tuple_size * capacity;
}

void SuperLargeHashTable::AddChunk(DataChunk &groups, DataChunk &payload) {
	if (groups.size() == 0) {
		return;
	}

	StaticPointerVector addresses;
	StaticVector<bool> new_group_dummy;

	FindOrCreateGroups(groups, addresses, new_group_dummy);

	// now every cell has an entry
	// update the aggregates
	Vector one(Value::BIGINT(1));
	index_t payload_idx = 0;

	for (index_t aggr_idx = 0; aggr_idx < aggregate_types.size(); aggr_idx++) {
		assert(payload.column_count > payload_idx);

		// for any entries for which a group was found, update the aggregate
		switch (aggregate_types[aggr_idx]) {
		case ExpressionType::AGGREGATE_COUNT_STAR:
			// add one to each address, regardless of if the value is NULL
			VectorOperations::Scatter::Add(one, addresses);
			break;
		case ExpressionType::AGGREGATE_COUNT:
			VectorOperations::Scatter::AddOne(payload.data[payload_idx], addresses);
			break;
		case ExpressionType::AGGREGATE_SUM:
			// addition
			VectorOperations::Scatter::Add(payload.data[payload_idx], addresses);
			break;
		case ExpressionType::AGGREGATE_MIN:
			// min
			VectorOperations::Scatter::Min(payload.data[payload_idx], addresses);
			break;
		case ExpressionType::AGGREGATE_MAX:
			// max
			VectorOperations::Scatter::Max(payload.data[payload_idx], addresses);
			break;
		case ExpressionType::AGGREGATE_FIRST:
			// first
			VectorOperations::Scatter::SetFirst(payload.data[payload_idx], addresses);
			break;
		case ExpressionType::AGGREGATE_SUM_DISTINCT:
		case ExpressionType::AGGREGATE_COUNT_DISTINCT: {
			assert(groups.sel_vector == payload.sel_vector);

			// construct chunk for secondary hash table probing
			vector<TypeId> probe_types(group_types);
			probe_types.push_back(payload_types[aggr_idx]);
			DataChunk probe_chunk;
			probe_chunk.Initialize(probe_types, false);
			for (index_t group_idx = 0; group_idx < group_types.size(); group_idx++) {
				probe_chunk.data[group_idx].Reference(groups.data[group_idx]);
			}
			probe_chunk.data[group_types.size()].Reference(payload.data[payload_idx]);
			probe_chunk.sel_vector = groups.sel_vector;
			probe_chunk.Verify();

			StaticPointerVector dummy_addresses;
			StaticVector<bool> probe_result;
			probe_result.count = payload.data[payload_idx].count;
			// this is the actual meat, find out which groups plus payload
			// value have not been seen yet
			distinct_hashes[aggr_idx]->FindOrCreateGroups(probe_chunk, dummy_addresses, probe_result);

			// now fix up the payload and addresses accordingly by creating
			// a selection vector
			sel_t distinct_sel_vector[STANDARD_VECTOR_SIZE];
			index_t match_count = 0;
			for (index_t probe_idx = 0; probe_idx < probe_result.count; probe_idx++) {
				index_t sel_idx = payload.sel_vector ? payload.sel_vector[probe_idx] : probe_idx;
				if (probe_result.data[sel_idx]) {
					distinct_sel_vector[match_count++] = sel_idx;
				}
			}

			Vector distinct_payload, distinct_addresses;
			distinct_payload.Reference(payload.data[payload_idx]);
			distinct_payload.sel_vector = distinct_sel_vector;
			distinct_payload.count = match_count;
			distinct_payload.Verify();

			distinct_addresses.Reference(addresses);
			distinct_addresses.sel_vector = distinct_sel_vector;
			distinct_addresses.count = match_count;
			distinct_addresses.Verify();

			if (aggregate_types[aggr_idx] == ExpressionType::AGGREGATE_COUNT_DISTINCT) {
				VectorOperations::Scatter::AddOne(distinct_payload, distinct_addresses);
			} else {
				VectorOperations::Scatter::Add(distinct_payload, distinct_addresses);
			}
			break;
		}
		case ExpressionType::AGGREGATE_STDDEV_SAMP: {
			// Streaming approximate standard deviation using Welford's
			// method, DOI: 10.2307/1266577

			// convert input to floating point if required
			Vector payload_double;
			if (payload.data[payload_idx].type != TypeId::DOUBLE) {
				payload_double.Initialize(TypeId::DOUBLE);
				VectorOperations::Cast(payload.data[payload_idx], payload_double);
			} else {
				payload_double.Reference(payload.data[payload_idx]);
			}

			VectorOperations::Exec(addresses, [&](index_t i, index_t k) {
				if (payload_double.nullmask[i]) {
					return;
				}
				// Layout of payload for STDDEV_SAMP: count(uint64_t), mean
				// (double), dsquared(double)

				auto base_ptr = ((data_ptr_t *)addresses.data)[i];
				auto count_ptr = (uint64_t *)base_ptr;
				auto mean_ptr = (double *)(base_ptr + sizeof(uint64_t));
				auto dsquared_ptr = (double *)(base_ptr + sizeof(uint64_t) + sizeof(double));

				// update running mean and d^2
				(*count_ptr)++;
				const double new_value = ((double *)payload_double.data)[i];
				const double mean_differential = (new_value - (*mean_ptr)) / (*count_ptr);
				const double new_mean = (*mean_ptr) + mean_differential;
				const double dsquared_increment = (new_value - new_mean) * (new_value - (*mean_ptr));
				const double new_dsquared = (*dsquared_ptr) + dsquared_increment;

				*mean_ptr = new_mean;
				*dsquared_ptr = new_dsquared;
				// see Scan() method below for final step
			});

			break;
		}
		default:
			throw NotImplementedException("Unimplemented aggregate type!");
		}

		// move to the next aggregate
		VectorOperations::AddInPlace(addresses,
		                             GetAggrPayloadSize(aggregate_types[aggr_idx], payload.data[payload_idx].type));
		payload_idx++;
	}
}

void SuperLargeHashTable::FetchAggregates(DataChunk &groups, DataChunk &result) {
	groups.Verify();
	assert(groups.column_count == group_types.size());
	for (index_t i = 0; i < result.column_count; i++) {
		result.data[i].count = groups.size();
		result.data[i].sel_vector = groups.data[0].sel_vector;
		assert(result.data[i].type == payload_types[i]);
	}
	result.sel_vector = groups.sel_vector;
	if (groups.size() == 0) {
		return;
	}
	// find the groups associated with the addresses
	// FIXME: this should not use the FindOrCreateGroups, creating them is unnecessary
	StaticPointerVector addresses;
	StaticVector<bool> new_group_dummy;
	FindOrCreateGroups(groups, addresses, new_group_dummy);
	// now fetch the aggregates
	for (index_t aggr_idx = 0; aggr_idx < aggregate_types.size(); aggr_idx++) {
		assert(result.column_count > aggr_idx);
		assert(aggregate_types[aggr_idx] == ExpressionType::AGGREGATE_COUNT_STAR ||
		       aggregate_types[aggr_idx] == ExpressionType::AGGREGATE_COUNT);
		assert(payload_types[aggr_idx] == TypeId::BIGINT);

		VectorOperations::Gather::Set(addresses, result.data[aggr_idx]);
		VectorOperations::AddInPlace(addresses,
		                             GetAggrPayloadSize(aggregate_types[aggr_idx], result.data[aggr_idx].type));
	}
}

template<class T>
void templated_compare_group_vector(data_ptr_t group_pointers[], Vector &groups, sel_t sel_vector[], index_t &sel_count, sel_t no_match_vector[], index_t &no_match_count) {
	auto data = (T*) groups.data;
	index_t current_count = 0;
	for(index_t i = 0; i < sel_count; i++) {
		index_t index = sel_vector[i];
		auto entry = group_pointers[index];
		if ((*((T*) entry)) == data[index]) {
			// match, continue to next group (if any)
			sel_vector[current_count++] = index;
		} else {
			// no match, move to next group
			no_match_vector[no_match_count++] = index;
		}
		group_pointers[index] += sizeof(T);
	}
	sel_count = current_count;
}

static void CompareGroupVector(data_ptr_t group_pointers[], Vector &groups, sel_t sel_vector[], index_t &sel_count, sel_t no_match_vector[], index_t &no_match_count) {
	switch(groups.type) {
		case TypeId::BOOLEAN:
		case TypeId::TINYINT:
			templated_compare_group_vector<int8_t>(group_pointers, groups, sel_vector, sel_count, no_match_vector, no_match_count);
			break;
		case TypeId::SMALLINT:
			templated_compare_group_vector<int16_t>(group_pointers, groups, sel_vector, sel_count, no_match_vector, no_match_count);
			break;
		case TypeId::INTEGER:
			templated_compare_group_vector<int32_t>(group_pointers, groups, sel_vector, sel_count, no_match_vector, no_match_count);
			break;
		case TypeId::BIGINT:
			templated_compare_group_vector<int64_t>(group_pointers, groups, sel_vector, sel_count, no_match_vector, no_match_count);
			break;
		case TypeId::FLOAT:
			templated_compare_group_vector<float>(group_pointers, groups, sel_vector, sel_count, no_match_vector, no_match_count);
			break;
		case TypeId::DOUBLE:
			templated_compare_group_vector<double>(group_pointers, groups, sel_vector, sel_count, no_match_vector, no_match_count);
			break;
		case TypeId::VARCHAR: {
			// compare group vector for varchar
			auto data = (const char**) groups.data;
			index_t current_count = 0;
			for(index_t i = 0; i < sel_count; i++) {
				index_t index = sel_vector[i];
				auto entry = group_pointers[index];
				if (strcmp(data[index], *((const char**) entry)) == 0) {
					// match, continue to next group (if any)
					sel_vector[current_count++] = index;
				} else {
					// no match, move to next group
					no_match_vector[no_match_count++] = index;
				}
				group_pointers[index] += sizeof(const char*);
			}
			sel_count = current_count;
			break;
		}
		default:
			throw Exception("Unsupported type for group vector");
	}
}

void SuperLargeHashTable::HashGroups(DataChunk &groups, Vector &addresses) {
	// create a set of hashes for the groups
	StaticVector<uint64_t> hashes;
	groups.Hash(hashes);

	auto data_pointers = (data_ptr_t *)addresses.data;

	// now compute the entry in the table based on the hash using a modulo
	// multiply the position by the tuple size and add the base address
	VectorOperations::ExecType<uint64_t>(hashes, [&](uint64_t element, index_t i, index_t k) {
		assert((element & bitmask) == (element % capacity));
		data_pointers[i] = data + ((element & bitmask) * tuple_size);
	});

	addresses.sel_vector = hashes.sel_vector;
	addresses.count = hashes.count;

	assert(addresses.sel_vector == groups.sel_vector);
}

// this is to support distinct aggregations where we need to record whether we
// have already seen a value for a group
void SuperLargeHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses, Vector &new_group) {
	// resize at 50% capacity, also need to fit the entire vector
	if (entries > capacity / 2 || capacity - entries <= STANDARD_VECTOR_SIZE) {
		Resize(capacity * 2);
	}

	// for each group, fill in the NULL value
	for(index_t group_idx = 0; group_idx < groups.column_count; group_idx++) {
		VectorOperations::FillNullMask(groups.data[group_idx]);
	}

	// we need to be able to fit at least one vector of data
	assert(capacity - entries > STANDARD_VECTOR_SIZE);
	assert(new_group.type == TypeId::BOOLEAN);
	assert(addresses.type == TypeId::POINTER);

	new_group.sel_vector = groups.data[0].sel_vector;

	HashGroups(groups, addresses);

	sel_t sel_vector[STANDARD_VECTOR_SIZE], empty_vector[STANDARD_VECTOR_SIZE];
	index_t sel_count = groups.size();
	VectorOperations::Exec(addresses, [&](index_t i, index_t k) {
		sel_vector[k] = i;
	});

	// list of addresses for the tuples
	auto data_pointers = (data_ptr_t *)addresses.data;
	if (parallel) {
		throw NotImplementedException("Parallel HT not implemented");
	}

	auto group_width = group_serializer.TupleSize();

	// zero initialize the new_groups array
	auto new_groups = ((bool*) new_group.data);
	memset(new_groups, 0, sizeof(bool) * STANDARD_VECTOR_SIZE);

	data_ptr_t group_pointers[STANDARD_VECTOR_SIZE];
	Vector pointers(TypeId::POINTER, (data_ptr_t) group_pointers);

	while(sel_count > 0) {
		index_t current_count = 0;
		index_t empty_count = 0;

		// first figure out for each remaining whether or not it belongs to a full or empty group
		for(index_t i = 0; i < sel_count; i++) {
			index_t index = sel_vector[i];
			auto entry = data_pointers[index];
			if (*entry == EMPTY_CELL) {
				// cell is empty; mark the cell as filled
				*entry = FULL_CELL;
				empty_vector[empty_count++] = index;
				new_groups[index] = true;
				// initialize the payload info for the column
				memcpy(entry + FLAG_SIZE + group_width, empty_payload_data.get(), payload_width);
			} else {
				// cell is occupied: add to check list
				sel_vector[current_count++] = index;
			}
			group_pointers[index] = entry + FLAG_SIZE;
			data_pointers[index] = entry + FLAG_SIZE + group_width;
		}
		sel_count = current_count;

		if (empty_count > 0) {
			// for each of the locations that are empty, serialize the group columns to the locations
			auto old_sel_vector = groups.sel_vector;
			index_t old_count = groups.size();
			for(index_t group_idx = 0; group_idx < groups.column_count; group_idx++) {
				// set up the new sel vector with the entries we need to write
				auto &group_column = groups.data[group_idx];
				group_column.sel_vector = empty_vector;
				group_column.count = empty_count;
				pointers.sel_vector = empty_vector;
				pointers.count = empty_count;

				VectorOperations::Scatter::SetAll(group_column, pointers);

				// restore the old sel_vector and count
				group_column.sel_vector = old_sel_vector;
				group_column.count = old_count;

				VectorOperations::AddInPlace(pointers, GetTypeIdSize(group_column.type));
			}
			entries += empty_count;
		}
		// now we have only the tuples remaining that might match to an existing group
		// start performing comparisons with each of the groups
		sel_t no_match_vector[STANDARD_VECTOR_SIZE];
		index_t no_match_count = 0;
		for (index_t group_idx = 0; group_idx < groups.column_count; group_idx++) {
			CompareGroupVector(group_pointers, groups.data[group_idx], sel_vector, sel_count, no_match_vector, no_match_count);
		}

		// each of the entries that do not match need to be moved to the next entry
		for(index_t i = 0; i < no_match_count; i++) {
			index_t index = no_match_vector[i];
			sel_vector[i] = index;
			data_pointers[index] += payload_width;
			assert(((uint64_t)(data_pointers[index] - data)) % tuple_size == 0);
			if (data_pointers[index] >= endptr) {
				data_pointers[index] = data;
			}
		}
		sel_count = no_match_count;
	}
}

index_t SuperLargeHashTable::Scan(index_t &scan_position, DataChunk &groups, DataChunk &result) {
	data_ptr_t ptr;
	data_ptr_t start = data + scan_position;
	data_ptr_t end = data + capacity * tuple_size;
	if (start >= end)
		return 0;

	Vector addresses(TypeId::POINTER, true, false);
	auto data_pointers = (data_ptr_t *)addresses.data;

	// scan the table for full cells starting from the scan position
	index_t entry = 0;
	for (ptr = start; ptr < end && entry < STANDARD_VECTOR_SIZE; ptr += tuple_size) {
		if (*ptr == FULL_CELL) {
			// found entry
			data_pointers[entry++] = ptr + FLAG_SIZE;
		}
	}
	if (entry == 0) {
		return 0;
	}
	addresses.count = entry;
	// fetch the group columns
	for (index_t i = 0; i < groups.column_count; i++) {
		auto &column = groups.data[i];
		column.count = entry;
		VectorOperations::Gather::Set(addresses, column);
		VectorOperations::AddInPlace(addresses, GetTypeIdSize(column.type));
	}

	for (index_t i = 0; i < aggregate_types.size(); i++) {
		auto &target = result.data[i];
		target.count = entry;
		switch (aggregate_types[i]) {
		case ExpressionType::AGGREGATE_STDDEV_SAMP: {
			// compute finalization of streaming stddev of sample
			VectorOperations::Exec(addresses, [&](uint64_t i, uint64_t k) {
				auto base_ptr = ((data_ptr_t *)addresses.data)[i];
				auto count_ptr = (uint64_t *)base_ptr;
				auto dsquared_ptr = (double *)(base_ptr + sizeof(uint64_t) + sizeof(double));

				if (*count_ptr == 0) {
					target.nullmask[i] = true;
					return;
				}
				double res = *count_ptr > 1 ? sqrt(*dsquared_ptr / (*count_ptr - 1)) : 0;

				((double *)target.data)[i] = res;
			});

			break;
		}
		default:
			VectorOperations::Gather::Set(addresses, target);
			break;
		}

		VectorOperations::AddInPlace(addresses, GetAggrPayloadSize(aggregate_types[i], target.type));
	}
	scan_position = ptr - data;
	return entry;
}
