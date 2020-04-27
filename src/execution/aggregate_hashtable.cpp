#include "duckdb/execution/aggregate_hashtable.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

#include <cmath>
#include <map>

using namespace duckdb;
using namespace std;

SuperLargeHashTable::SuperLargeHashTable(idx_t initial_capacity, vector<TypeId> group_types,
                                         vector<TypeId> payload_types, vector<BoundAggregateExpression *> bindings,
                                         bool parallel)
    : SuperLargeHashTable(initial_capacity, move(group_types), move(payload_types),
                          AggregateObject::CreateAggregateObjects(move(bindings)), parallel) {
}

vector<AggregateObject> AggregateObject::CreateAggregateObjects(vector<BoundAggregateExpression *> bindings) {
	vector<AggregateObject> aggregates;
	for (auto &binding : bindings) {
		auto payload_size = binding->function.state_size();
		aggregates.push_back(AggregateObject(binding->function, binding->children.size(), payload_size,
		                                     binding->distinct, binding->return_type));
	}
	return aggregates;
}

SuperLargeHashTable::SuperLargeHashTable(idx_t initial_capacity, vector<TypeId> group_types,
                                         vector<TypeId> payload_types, vector<AggregateObject> aggregate_objects,
                                         bool parallel)
    : aggregates(move(aggregate_objects)), group_types(group_types), payload_types(payload_types), group_width(0),
      payload_width(0), capacity(0), entries(0), data(nullptr), parallel(parallel) {
	// HT tuple layout is as follows:
	// [FLAG][GROUPS][PAYLOAD]
	// [FLAG] is the state of the tuple in memory
	// [GROUPS] is the groups
	// [PAYLOAD] is the payload (i.e. the aggregate states)
	for (idx_t i = 0; i < group_types.size(); i++) {
		group_width += GetTypeIdSize(group_types[i]);
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		payload_width += aggregates[i].payload_size;
	}
	empty_payload_data = unique_ptr<data_t[]>(new data_t[payload_width]);
	// initialize the aggregates to the NULL value
	auto pointer = empty_payload_data.get();
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i];
		aggr.function.initialize(pointer);
		pointer += aggr.payload_size;
	}

	// FIXME: this always creates this vector, even if no distinct if present.
	// it likely does not matter.
	distinct_hashes.resize(aggregates.size());

	// create additional hash tables for distinct aggrs
	idx_t payload_idx = 0;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i];
		if (aggr.distinct) {
			// group types plus aggr return type
			vector<TypeId> distinct_group_types(group_types);
			vector<TypeId> distinct_payload_types;
			vector<BoundAggregateExpression *> distinct_aggregates;
			distinct_group_types.push_back(payload_types[payload_idx]);
			distinct_hashes[i] = make_unique<SuperLargeHashTable>(initial_capacity, distinct_group_types,
			                                                      distinct_payload_types, distinct_aggregates);
		}
		if (aggr.child_count) {
			payload_idx += aggr.child_count;
		} else {
			payload_idx += 1;
		}
	}

	tuple_size = FLAG_SIZE + (group_width + payload_width);
	Resize(initial_capacity);
}

SuperLargeHashTable::~SuperLargeHashTable() {
	Destroy();
}

void SuperLargeHashTable::CallDestructors(Vector &state_vector, idx_t count) {
	if (count == 0) {
		return;
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i];
		if (aggr.function.destructor) {
			aggr.function.destructor(state_vector, count);
		}
		// move to the next aggregate state
		VectorOperations::AddInPlace(state_vector, aggr.payload_size, count);
	}
}

void SuperLargeHashTable::Destroy() {
	if (!data) {
		return;
	}
	// check if there is a destructor
	bool has_destructor = false;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		if (aggregates[i].function.destructor) {
			has_destructor = true;
		}
	}
	if (!has_destructor) {
		return;
	}
	// there are aggregates with destructors: loop over the hash table
	// and call the destructor method for each of the aggregates
	data_ptr_t data_pointers[STANDARD_VECTOR_SIZE];
	Vector state_vector(TypeId::POINTER, (data_ptr_t)data_pointers);
	idx_t count = 0;
	for (data_ptr_t ptr = data, end = data + capacity * tuple_size; ptr < end; ptr += tuple_size) {
		if (*ptr == FULL_CELL) {
			// found entry
			data_pointers[count++] = ptr + FLAG_SIZE + group_width;
			if (count == STANDARD_VECTOR_SIZE) {
				// vector is full: call the destructors
				CallDestructors(state_vector, count);
				count = 0;
			}
		}
	}
	CallDestructors(state_vector, count);
}

void SuperLargeHashTable::Resize(idx_t size) {
	if (size <= capacity) {
		throw Exception("Cannot downsize a hash table!");
	}
	if (size < STANDARD_VECTOR_SIZE) {
		size = STANDARD_VECTOR_SIZE;
	}
	// size needs to be a power of 2
	assert((size & (size - 1)) == 0);
	bitmask = size - 1;

	if (entries > 0) {
		auto new_table = make_unique<SuperLargeHashTable>(size, group_types, payload_types, aggregates, parallel);

		DataChunk groups;
		groups.Initialize(group_types);

		Vector addresses(TypeId::POINTER);
		auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

		data_ptr_t ptr = data;
		data_ptr_t end = data + capacity * tuple_size;

		assert(new_table->tuple_size == this->tuple_size);

		while (true) {
			groups.Reset();

			// scan the table for full cells starting from the scan position
			idx_t found_entries = 0;
			for (; ptr < end && found_entries < STANDARD_VECTOR_SIZE; ptr += tuple_size) {
				if (*ptr == FULL_CELL) {
					// found entry
					data_pointers[found_entries++] = ptr + FLAG_SIZE;
				}
			}
			if (found_entries == 0) {
				break;
			}
			// fetch the group columns
			groups.SetCardinality(found_entries);
			for (idx_t i = 0; i < groups.column_count(); i++) {
				auto &column = groups.data[i];
				VectorOperations::Gather::Set(addresses, column, found_entries);
			}

			groups.Verify();
			assert(groups.size() == found_entries);
			Vector new_addresses(TypeId::POINTER);
			new_table->FindOrCreateGroups(groups, new_addresses);

			// NB: both address vectors already point to the payload start
			assert(addresses.type == new_addresses.type && addresses.type == TypeId::POINTER);

			auto new_address_data = FlatVector::GetData<data_ptr_t>(new_addresses);
			for (idx_t i = 0; i < found_entries; i++) {
				memcpy(new_address_data[i], data_pointers[i], payload_width);
			}
		}

		assert(this->entries == new_table->entries);

		this->data = move(new_table->data);
		this->owned_data = move(new_table->owned_data);
		this->capacity = new_table->capacity;
		this->string_heap.MergeHeap(new_table->string_heap);
		new_table->data = nullptr;
	} else {
		data = new data_t[size * tuple_size];
		owned_data = unique_ptr<data_t[]>(data);
		for (idx_t i = 0; i < size; i++) {
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

	Vector addresses(TypeId::POINTER);
	FindOrCreateGroups(groups, addresses);

	// now every cell has an entry
	// update the aggregates
	idx_t payload_idx = 0;

	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		assert(payload.column_count() > payload_idx);

		// for any entries for which a group was found, update the aggregate
		auto &aggr = aggregates[aggr_idx];
		auto input_count = max((idx_t)1, (idx_t)aggr.child_count);
		if (aggr.distinct) {
			// construct chunk for secondary hash table probing
			vector<TypeId> probe_types(group_types);
			for (idx_t i = 0; i < aggr.child_count; i++) {
				probe_types.push_back(payload_types[payload_idx]);
			}
			DataChunk probe_chunk;
			probe_chunk.Initialize(probe_types);
			for (idx_t group_idx = 0; group_idx < group_types.size(); group_idx++) {
				probe_chunk.data[group_idx].Reference(groups.data[group_idx]);
			}
			for (idx_t i = 0; i < aggr.child_count; i++) {
				probe_chunk.data[group_types.size() + i].Reference(payload.data[payload_idx + i]);
			}
			probe_chunk.SetCardinality(groups);
			probe_chunk.Verify();

			Vector dummy_addresses(TypeId::POINTER);
			SelectionVector new_groups(STANDARD_VECTOR_SIZE);
			// this is the actual meat, find out which groups plus payload
			// value have not been seen yet
			idx_t new_group_count =
			    distinct_hashes[aggr_idx]->FindOrCreateGroups(probe_chunk, dummy_addresses, new_groups);

			// now fix up the payload and addresses accordingly by creating
			// a selection vector
			if (new_group_count > 0) {
				Vector distinct_addresses;
				distinct_addresses.Slice(addresses, new_groups, new_group_count);
				for (idx_t i = 0; i < aggr.child_count; i++) {
					payload.data[payload_idx + i].Slice(new_groups, new_group_count);
					payload.data[payload_idx + i].Verify(new_group_count);
				}

				distinct_addresses.Verify(new_group_count);

				aggr.function.update(&payload.data[payload_idx], input_count, distinct_addresses, new_group_count);
			}
		} else {
			aggr.function.update(&payload.data[payload_idx], input_count, addresses, payload.size());
		}

		// move to the next aggregate
		payload_idx += input_count;
		VectorOperations::AddInPlace(addresses, aggr.payload_size, payload.size());
	}
}

void SuperLargeHashTable::FetchAggregates(DataChunk &groups, DataChunk &result) {
	groups.Verify();
	assert(groups.column_count() == group_types.size());
	for (idx_t i = 0; i < result.column_count(); i++) {
		assert(result.data[i].type == payload_types[i]);
	}
	result.SetCardinality(groups);
	if (groups.size() == 0) {
		return;
	}
	// find the groups associated with the addresses
	// FIXME: this should not use the FindOrCreateGroups, creating them is unnecessary
	Vector addresses(TypeId::POINTER);
	FindOrCreateGroups(groups, addresses);
	// now fetch the aggregates
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		assert(result.column_count() > aggr_idx);
		assert(payload_types[aggr_idx] == TypeId::INT64);

		VectorOperations::Gather::Set(addresses, result.data[aggr_idx], groups.size());
	}
}

void SuperLargeHashTable::HashGroups(DataChunk &groups, Vector &addresses) {
	// create a set of hashes for the groups
	Vector hashes(TypeId::HASH);
	groups.Hash(hashes);

	// now compute the entry in the table based on the hash using a modulo
	// multiply the position by the tuple size and add the base address
	UnaryExecutor::Execute<hash_t, data_ptr_t>(hashes, addresses, groups.size(), [&](hash_t element) {
		assert((element & bitmask) == (element % capacity));
		return data + ((element & bitmask) * tuple_size);
	});
}

template <class T>
static void templated_scatter(VectorData &gdata, Vector &addresses, const SelectionVector &sel, idx_t count,
                              idx_t type_size) {
	auto data = (T *)gdata.data;
	auto pointers = FlatVector::GetData<uintptr_t>(addresses);
	if (gdata.nullmask->any()) {
		for (idx_t i = 0; i < count; i++) {
			auto pointer_idx = sel.get_index(i);
			auto group_idx = gdata.sel->get_index(pointer_idx);
			auto ptr = (T *)pointers[pointer_idx];

			if ((*gdata.nullmask)[group_idx]) {
				*ptr = NullValue<T>();
			} else {
				*ptr = data[group_idx];
			}
			pointers[pointer_idx] += type_size;
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto pointer_idx = sel.get_index(i);
			auto group_idx = gdata.sel->get_index(pointer_idx);
			auto ptr = (T *)pointers[pointer_idx];

			*ptr = data[group_idx];
			pointers[pointer_idx] += type_size;
		}
	}
}

void SuperLargeHashTable::ScatterGroups(DataChunk &groups, unique_ptr<VectorData[]> &group_data, Vector &addresses,
                                        const SelectionVector &sel, idx_t count) {
	for (idx_t grp_idx = 0; grp_idx < groups.column_count(); grp_idx++) {
		auto &data = groups.data[grp_idx];
		auto &gdata = group_data[grp_idx];

		auto type_size = GetTypeIdSize(data.type);

		switch (data.type) {
		case TypeId::BOOL:
		case TypeId::INT8:
			templated_scatter<int8_t>(gdata, addresses, sel, count, type_size);
			break;
		case TypeId::INT16:
			templated_scatter<int16_t>(gdata, addresses, sel, count, type_size);
			break;
		case TypeId::INT32:
			templated_scatter<int32_t>(gdata, addresses, sel, count, type_size);
			break;
		case TypeId::INT64:
			templated_scatter<int64_t>(gdata, addresses, sel, count, type_size);
			break;
		case TypeId::FLOAT:
			templated_scatter<float>(gdata, addresses, sel, count, type_size);
			break;
		case TypeId::DOUBLE:
			templated_scatter<double>(gdata, addresses, sel, count, type_size);
			break;
		case TypeId::VARCHAR: {
			auto data = (string_t *)gdata.data;
			auto pointers = FlatVector::GetData<uintptr_t>(addresses);

			for (idx_t i = 0; i < count; i++) {
				auto pointer_idx = sel.get_index(i);
				auto group_idx = gdata.sel->get_index(pointer_idx);
				auto ptr = (string_t *)pointers[pointer_idx];

				if ((*gdata.nullmask)[group_idx]) {
					*ptr = NullValue<string_t>();
				} else if (data[group_idx].IsInlined()) {
					*ptr = data[group_idx];
				} else {
					*ptr = string_heap.AddString(data[group_idx]);
				}
				pointers[pointer_idx] += type_size;
			}
			break;
		}
		default:
			throw Exception("Unsupported type for group vector");
		}
	}
}

template <class T>
static void templated_compare_groups(VectorData &gdata, Vector &addresses, SelectionVector &sel, idx_t &count,
                                     idx_t type_size, SelectionVector &no_match, idx_t &no_match_count) {
	auto data = (T *)gdata.data;
	auto pointers = FlatVector::GetData<uintptr_t>(addresses);
	idx_t match_count = 0;
	if (gdata.nullmask->any()) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto group_idx = gdata.sel->get_index(idx);
			auto value = (T *)pointers[idx];

			if ((*gdata.nullmask)[group_idx]) {
				if (IsNullValue<T>(*value)) {
					// match: move to next value to compare
					sel.set_index(match_count++, idx);
					pointers[idx] += type_size;
				} else {
					no_match.set_index(no_match_count++, idx);
				}
			} else {
				if (Equals::Operation<T>(data[group_idx], *value)) {
					sel.set_index(match_count++, idx);
					pointers[idx] += type_size;
				} else {
					no_match.set_index(no_match_count++, idx);
				}
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto group_idx = gdata.sel->get_index(idx);
			auto value = (T *)pointers[idx];

			if (Equals::Operation<T>(data[group_idx], *value)) {
				sel.set_index(match_count++, idx);
				pointers[idx] += type_size;
			} else {
				no_match.set_index(no_match_count++, idx);
			}
		}
	}
	count = match_count;
}

static idx_t CompareGroups(DataChunk &groups, unique_ptr<VectorData[]> &group_data, Vector &addresses,
                           SelectionVector &sel, idx_t count, SelectionVector &no_match) {
	idx_t no_match_count = 0;
	for (idx_t group_idx = 0; group_idx < groups.column_count(); group_idx++) {
		auto &data = groups.data[group_idx];
		auto &gdata = group_data[group_idx];
		auto type_size = GetTypeIdSize(data.type);
		switch (data.type) {
		case TypeId::BOOL:
		case TypeId::INT8:
			templated_compare_groups<int8_t>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case TypeId::INT16:
			templated_compare_groups<int16_t>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case TypeId::INT32:
			templated_compare_groups<int32_t>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case TypeId::INT64:
			templated_compare_groups<int64_t>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case TypeId::FLOAT:
			templated_compare_groups<float>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case TypeId::DOUBLE:
			templated_compare_groups<double>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case TypeId::VARCHAR:
			templated_compare_groups<string_t>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		default:
			throw Exception("Unsupported type for group vector");
		}
	}
	return no_match_count;
}

// this is to support distinct aggregations where we need to record whether we
// have already seen a value for a group
idx_t SuperLargeHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses, SelectionVector &new_groups) {
	// resize at 50% capacity, also need to fit the entire vector
	if (entries > capacity / 2 || capacity - entries <= STANDARD_VECTOR_SIZE) {
		Resize(capacity * 2);
	}

	// we need to be able to fit at least one vector of data
	assert(capacity - entries > STANDARD_VECTOR_SIZE);
	assert(addresses.type == TypeId::POINTER);

	// hash the groups to get the addresses
	HashGroups(groups, addresses);

	addresses.Normalify(groups.size());
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	data_ptr_t group_pointers[STANDARD_VECTOR_SIZE];
	Vector pointers(TypeId::POINTER, (data_ptr_t)group_pointers);

	// set up the selection vectors
	SelectionVector v1(STANDARD_VECTOR_SIZE);
	SelectionVector v2(STANDARD_VECTOR_SIZE);
	SelectionVector empty_vector(STANDARD_VECTOR_SIZE);

	// we start out with all entries [0, 1, 2, ..., groups.size()]
	const SelectionVector *sel_vector = &FlatVector::IncrementalSelectionVector;
	SelectionVector *next_vector = &v1;
	SelectionVector *no_match_vector = &v2;
	idx_t remaining_entries = groups.size();

	// orrify all the groups
	auto group_data = unique_ptr<VectorData[]>(new VectorData[groups.column_count()]);
	for (idx_t grp_idx = 0; grp_idx < groups.column_count(); grp_idx++) {
		groups.data[grp_idx].Orrify(groups.size(), group_data[grp_idx]);
	}

	idx_t new_group_count = 0;
	while (remaining_entries > 0) {
		idx_t entry_count = 0;
		idx_t empty_count = 0;

		// first figure out for each remaining whether or not it belongs to a full or empty group
		for (idx_t i = 0; i < remaining_entries; i++) {
			idx_t index = sel_vector->get_index(i);
			auto entry = data_pointers[index];
			if (*entry == EMPTY_CELL) {
				// cell is empty; mark the cell as filled
				*entry = FULL_CELL;
				empty_vector.set_index(empty_count++, index);
				new_groups.set_index(new_group_count++, index);
				// initialize the payload info for the column
				memcpy(entry + FLAG_SIZE + group_width, empty_payload_data.get(), payload_width);
			} else {
				// cell is occupied: add to check list
				next_vector->set_index(entry_count++, index);
			}
			group_pointers[index] = entry + FLAG_SIZE;
			data_pointers[index] = entry + FLAG_SIZE + group_width;
		}

		if (empty_count > 0) {
			// for each of the locations that are empty, serialize the group columns to the locations
			ScatterGroups(groups, group_data, pointers, empty_vector, empty_count);
			entries += empty_count;
		}
		// now we have only the tuples remaining that might match to an existing group
		// start performing comparisons with each of the groups
		idx_t no_match_count = CompareGroups(groups, group_data, pointers, *next_vector, entry_count, *no_match_vector);

		// each of the entries that do not match we move them to the next entry in the HT
		for (idx_t i = 0; i < no_match_count; i++) {
			idx_t index = no_match_vector->get_index(i);
			data_pointers[index] += payload_width;
			assert(((uint64_t)(data_pointers[index] - data)) % tuple_size == 0);
			if (data_pointers[index] >= endptr) {
				data_pointers[index] = data;
			}
		}
		sel_vector = no_match_vector;
		std::swap(next_vector, no_match_vector);
		remaining_entries = no_match_count;
	}
	return new_group_count;
}

void SuperLargeHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses) {
	// create a dummy new_groups sel vector
	SelectionVector new_groups(STANDARD_VECTOR_SIZE);
	FindOrCreateGroups(groups, addresses, new_groups);
}

idx_t SuperLargeHashTable::Scan(idx_t &scan_position, DataChunk &groups, DataChunk &result) {
	data_ptr_t ptr;
	data_ptr_t start = data + scan_position;
	data_ptr_t end = data + capacity * tuple_size;
	if (start >= end) {
		return 0;
	}

	Vector addresses(TypeId::POINTER);
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	// scan the table for full cells starting from the scan position
	idx_t entry = 0;
	for (ptr = start; ptr < end && entry < STANDARD_VECTOR_SIZE; ptr += tuple_size) {
		if (*ptr == FULL_CELL) {
			// found entry
			data_pointers[entry++] = ptr + FLAG_SIZE;
		}
	}
	if (entry == 0) {
		return 0;
	}
	groups.SetCardinality(entry);
	result.SetCardinality(entry);
	// fetch the group columns
	for (idx_t i = 0; i < groups.column_count(); i++) {
		auto &column = groups.data[i];
		VectorOperations::Gather::Set(addresses, column, groups.size());
	}

	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &target = result.data[i];
		auto &aggr = aggregates[i];
		aggr.function.finalize(addresses, target, groups.size());

		VectorOperations::AddInPlace(addresses, aggr.payload_size, groups.size());
	}
	scan_position = ptr - data;
	return entry;
}
