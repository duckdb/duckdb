#include "duckdb/execution/aggregate_hashtable.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cmath>
#include <map>

namespace duckdb {
using namespace std;

GroupedAggregateHashTable::GroupedAggregateHashTable(BufferManager &buffer_manager, idx_t initial_capacity,
                                                     vector<LogicalType> group_types, vector<LogicalType> payload_types,
                                                     vector<BoundAggregateExpression *> bindings)
    : GroupedAggregateHashTable(buffer_manager, initial_capacity, move(group_types), move(payload_types),
                                AggregateObject::CreateAggregateObjects(move(bindings))) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(BufferManager &buffer_manager, idx_t initial_capacity,
                                                     vector<LogicalType> group_types)
    : GroupedAggregateHashTable(buffer_manager, initial_capacity, move(group_types), {},
                                (vector<BoundAggregateExpression *>){}) {
}

vector<AggregateObject> AggregateObject::CreateAggregateObjects(vector<BoundAggregateExpression *> bindings) {
	vector<AggregateObject> aggregates;
	for (auto &binding : bindings) {
		auto payload_size = binding->function.state_size();
		aggregates.push_back(AggregateObject(binding->function, binding->children.size(), payload_size,
		                                     binding->distinct, binding->return_type.InternalType()));
	}
	return aggregates;
}

GroupedAggregateHashTable::GroupedAggregateHashTable(BufferManager &buffer_manager, idx_t initial_capacity,
                                                     vector<LogicalType> group_types, vector<LogicalType> payload_types,
                                                     vector<AggregateObject> aggregate_objects)
    : buffer_manager(buffer_manager), aggregates(move(aggregate_objects)), group_types(group_types),
      payload_types(payload_types), group_width(0), payload_width(0), capacity(0), entries(0), payload_block_idx(0),
      finalized(false) {

	for (idx_t i = 0; i < group_types.size(); i++) {
		group_width += GetTypeIdSize(group_types[i].InternalType());
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

	assert(group_width > 0);

	// HT layout
	hash_width = sizeof(hash_t);
	tuple_size = hash_width + group_width + payload_width;
	assert(tuple_size <= Storage::BLOCK_ALLOC_SIZE);

	tuples_per_block = Storage::BLOCK_ALLOC_SIZE / tuple_size;
	hash_prefix_get_bitmask = ((hash_t)-1 << ((hash_width - sizeof(uint16_t)) * 8));

	hashes_hdl = buffer_manager.Allocate(Storage::BLOCK_ALLOC_SIZE);
	Resize(initial_capacity);

	// create additional hash tables for distinct aggrs
	distinct_hashes.resize(aggregates.size());

	idx_t payload_idx = 0;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i];
		if (aggr.distinct) {
			// group types plus aggr return type
			vector<LogicalType> distinct_group_types(group_types);
			for (idx_t child_idx = 0; child_idx < aggr.child_count; child_idx++) {
				distinct_group_types.push_back(payload_types[payload_idx]);
			}
			distinct_hashes[i] =
			    make_unique<GroupedAggregateHashTable>(buffer_manager, initial_capacity, distinct_group_types);
		}
		if (aggr.child_count) {
			payload_idx += aggr.child_count;
		} else {
			payload_idx += 1;
		}
	}
}

GroupedAggregateHashTable::~GroupedAggregateHashTable() {
	Destroy();
}

void GroupedAggregateHashTable::NewBlock() {
	payload_hds.push_back(buffer_manager.Allocate(Storage::BLOCK_ALLOC_SIZE, true));
	payload_block_idx = 0;
}

void GroupedAggregateHashTable::CallDestructors(Vector &state_vector, idx_t count) {
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

void GroupedAggregateHashTable::Destroy() {
	if (!hashes_hdl) {
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
	Vector state_vector(LogicalType::POINTER, (data_ptr_t)data_pointers);
	idx_t count = 0;

	idx_t destroy_entries = entries;
	for (auto &payload_chunk : payload_hds) {
		auto this_entries = MinValue(tuples_per_block, destroy_entries);
		for (data_ptr_t ptr = payload_chunk->Ptr(), end = payload_chunk->Ptr() + this_entries * tuple_size; ptr < end;
		     ptr += tuple_size) {
			// found entry
			data_pointers[count++] = ptr + hash_width + group_width;
			if (count == STANDARD_VECTOR_SIZE) {
				// vector is full: call the destructors
				CallDestructors(state_vector, count);
				count = 0;
			}
		}
		destroy_entries -= this_entries;
	}
	CallDestructors(state_vector, count);
}

void GroupedAggregateHashTable::Verify() {
#ifdef DEBUG
	auto hash_ptr = (aggr_ht_entry_64 *)hashes_hdl->Ptr();
	idx_t count = 0;
	for (idx_t i = 0; i < capacity; i++) {
		if (hash_ptr[i].page_nr > 0) {
			assert((hash_ptr[i].salt) == (*(hash_t *)GetPtr(hash_ptr[i]) >> hash_prefix_shift));
			count++;
		}
	}
	assert(count == entries);
#endif
}

void GroupedAggregateHashTable::Resize(idx_t size) {
	Verify();

	if (size <= capacity) {
		throw Exception("Cannot downsize a hash table!");
	}
	if (size < STANDARD_VECTOR_SIZE) {
		size = STANDARD_VECTOR_SIZE;
	}

	// size needs to be a power of 2
	assert((size & (size - 1)) == 0);
	bitmask = size - 1;

	auto byte_size = size * sizeof(aggr_ht_entry_64);

	if (size * sizeof(data_ptr_t) > (idx_t)Storage::BLOCK_ALLOC_SIZE) {
		hashes_hdl = buffer_manager.Allocate(byte_size);
	}
	auto hashes = hashes_hdl->Ptr();
	memset(hashes, 0, byte_size);
	hashes_end_ptr = hashes + byte_size;
	capacity = size;

	auto hashes_arr = (aggr_ht_entry_64 *)hashes;

	uint16_t payload_page_idx = 1;
	uint32_t payload_page_offset;

	if (entries > 0) {
		idx_t resize_entries = entries;
		for (auto &payload_chunk : payload_hds) {
			payload_page_offset = 0;

			auto this_entries = MinValue(tuples_per_block, resize_entries);
			for (data_ptr_t ptr = payload_chunk->Ptr(), end = payload_chunk->Ptr() + this_entries * tuple_size;
			     ptr < end; ptr += tuple_size) {
				auto entry_hash = *(hash_t *)ptr;
				assert((entry_hash & bitmask) == (entry_hash % capacity));
				auto entry_idx = (idx_t)entry_hash & bitmask;
				while (hashes_arr[entry_idx].page_nr > 0) {
					entry_idx++;
					if (entry_idx >= capacity) {
						entry_idx = 0;
					}
				}

				assert(!hashes_arr[entry_idx].page_nr);
				assert(entry_hash >> hash_prefix_shift <= NumericLimits<uint16_t>::Maximum());

				hashes_arr[entry_idx].salt = entry_hash >> hash_prefix_shift;
				hashes_arr[entry_idx].page_nr = payload_page_idx;
				hashes_arr[entry_idx].page_offset = payload_page_offset;

				payload_page_offset++;
			}
			resize_entries -= this_entries;
			payload_page_idx++;
		}
		assert(resize_entries == 0);
	}

	Verify();
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload) {
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);

	return AddChunk(groups, hashes, payload);
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, Vector &group_hashes, DataChunk &payload) {
	if (finalized) {
		throw InternalException("HT already finalized");
	}

	if (groups.size() == 0) {
		return 0;
	}
	// dummy
	SelectionVector new_groups(STANDARD_VECTOR_SIZE);

	Vector addresses(LogicalType::POINTER);
	auto new_group_count = FindOrCreateGroups(groups, group_hashes, addresses, new_groups);

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
			vector<LogicalType> probe_types(group_types);
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

			Vector dummy_addresses(LogicalType::POINTER);
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

	Verify();
	return new_group_count;
}

void GroupedAggregateHashTable::FetchAggregates(DataChunk &groups, DataChunk &result) {
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
	Vector addresses(LogicalType::POINTER);
	FindOrCreateGroups(groups, addresses);
	// now fetch the aggregates
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		assert(result.column_count() > aggr_idx);

		VectorOperations::Gather::Set(addresses, result.data[aggr_idx], groups.size());
	}
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

void GroupedAggregateHashTable::ScatterGroups(DataChunk &groups, unique_ptr<VectorData[]> &group_data,
                                              Vector &addresses, const SelectionVector &sel, idx_t count) {
	if (count == 0) {
		return;
	}
	for (idx_t grp_idx = 0; grp_idx < groups.column_count(); grp_idx++) {
		auto &data = groups.data[grp_idx];
		auto &gdata = group_data[grp_idx];

		auto type_size = GetTypeIdSize(data.type.InternalType());

		switch (data.type.InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			templated_scatter<int8_t>(gdata, addresses, sel, count, type_size);
			break;
		case PhysicalType::INT16:
			templated_scatter<int16_t>(gdata, addresses, sel, count, type_size);
			break;
		case PhysicalType::INT32:
			templated_scatter<int32_t>(gdata, addresses, sel, count, type_size);
			break;
		case PhysicalType::INT64:
			templated_scatter<int64_t>(gdata, addresses, sel, count, type_size);
			break;
		case PhysicalType::INT128:
			templated_scatter<hugeint_t>(gdata, addresses, sel, count, type_size);
			break;
		case PhysicalType::FLOAT:
			templated_scatter<float>(gdata, addresses, sel, count, type_size);
			break;
		case PhysicalType::DOUBLE:
			templated_scatter<double>(gdata, addresses, sel, count, type_size);
			break;
		case PhysicalType::INTERVAL:
			templated_scatter<interval_t>(gdata, addresses, sel, count, type_size);
			break;
		case PhysicalType::VARCHAR: {
			auto string_data = (string_t *)gdata.data;
			auto pointers = FlatVector::GetData<uintptr_t>(addresses);

			for (idx_t i = 0; i < count; i++) {
				auto pointer_idx = sel.get_index(i);
				auto group_idx = gdata.sel->get_index(pointer_idx);
				auto ptr = (string_t *)pointers[pointer_idx];

				if ((*gdata.nullmask)[group_idx]) {
					*ptr = NullValue<string_t>();
				} else if (string_data[group_idx].IsInlined()) {
					*ptr = string_data[group_idx];
				} else {
					*ptr = string_heap.AddString(string_data[group_idx]);
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

static void CompareGroups(DataChunk &groups, unique_ptr<VectorData[]> &group_data, Vector &addresses,
                          SelectionVector &sel, idx_t count, SelectionVector &no_match, idx_t &no_match_count) {
	for (idx_t group_idx = 0; group_idx < groups.column_count(); group_idx++) {
		auto &data = groups.data[group_idx];
		auto &gdata = group_data[group_idx];
		auto type_size = GetTypeIdSize(data.type.InternalType());
		switch (data.type.InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			templated_compare_groups<int8_t>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case PhysicalType::INT16:
			templated_compare_groups<int16_t>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case PhysicalType::INT32:
			templated_compare_groups<int32_t>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case PhysicalType::INT64:
			templated_compare_groups<int64_t>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case PhysicalType::INT128:
			templated_compare_groups<hugeint_t>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case PhysicalType::FLOAT:
			templated_compare_groups<float>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case PhysicalType::DOUBLE:
			templated_compare_groups<double>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case PhysicalType::INTERVAL:
			templated_compare_groups<interval_t>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		case PhysicalType::VARCHAR:
			templated_compare_groups<string_t>(gdata, addresses, sel, count, type_size, no_match, no_match_count);
			break;
		default:
			throw Exception("Unsupported type for group vector");
		}
	}
}

data_ptr_t GroupedAggregateHashTable::GetPtr(aggr_ht_entry_64 &ht_entry_val) {
	assert(ht_entry_val.page_nr > 0);
	return payload_hds[ht_entry_val.page_nr - 1]->Ptr() + ((ht_entry_val.page_offset) * tuple_size);
}

// this is to support distinct aggregations where we need to record whether we
// have already seen a value for a group
idx_t GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &group_hashes, Vector &addresses_out,
                                                    SelectionVector &new_groups_out) {
	// resize at 50% capacity, also need to fit the entire vector
	if (entries > capacity / 2 || capacity - entries <= groups.size()) {
		Resize(capacity * 4);
	}

	assert(groups.column_count() == group_types.size());
	// we need to be able to fit at least one vector of data
	assert(capacity - entries >= groups.size());
	assert(group_hashes.type == LogicalType::HASH);

	group_hashes.Normalify(groups.size());
	auto group_hashes_ptr = FlatVector::GetData<hash_t>(group_hashes);

	Vector ht_offsets(LogicalTypeId::BIGINT);
	assert(ht_offsets.vector_type == VectorType::FLAT_VECTOR);

	// now compute the entry in the table based on the hash using a modulo
	// multiply the position by the tuple size and add the base address
	UnaryExecutor::Execute<hash_t, uint64_t>(group_hashes, ht_offsets, groups.size(), [&](hash_t element) {
		assert((element & bitmask) == (element % capacity));
		return (element & bitmask);
	});

	auto ht_offsets_ptr = FlatVector::GetData<uint64_t>(ht_offsets);

	Vector group_pointers(LogicalType::POINTER);
	auto group_pointers_ptr = FlatVector::GetData<data_ptr_t>(group_pointers);

	// set up the selection vectors
	SelectionVector v1(STANDARD_VECTOR_SIZE);
	SelectionVector v2(STANDARD_VECTOR_SIZE);
	SelectionVector empty_vector(STANDARD_VECTOR_SIZE);

	// we start out with all entries [0, 1, 2, ..., groups.size()]
	const SelectionVector *sel_vector = &FlatVector::IncrementalSelectionVector;
	SelectionVector *group_compare_vector = &v1;
	SelectionVector *no_match_vector = &v2;
	idx_t remaining_entries = groups.size();

	// orrify all the groups
	auto group_data = unique_ptr<VectorData[]>(new VectorData[groups.column_count()]);
	for (idx_t grp_idx = 0; grp_idx < groups.column_count(); grp_idx++) {
		groups.data[grp_idx].Orrify(groups.size(), group_data[grp_idx]);
	}

	idx_t new_group_count = 0;
	while (remaining_entries > 0) {
		idx_t new_entry_count = 0;
		idx_t need_compare_count = 0;
		idx_t no_match_count = 0;

		// first figure out for each remaining whether or not it belongs to a full or empty group
		for (idx_t i = 0; i < remaining_entries; i++) {
			idx_t index = sel_vector->get_index(i);
			auto ht_entry_ptr = ((aggr_ht_entry_64 *)this->hashes_hdl->Ptr()) + ht_offsets_ptr[index];
			if (ht_entry_ptr->page_nr == 0) { // we use page number 0 as a "unused marker"
				// cell is empty; setup the new entry
				if (payload_block_idx == tuples_per_block || payload_hds.empty()) {
					NewBlock();
				}

				auto entry_payload_ptr = payload_hds.back()->Ptr() + (payload_block_idx * tuple_size);

				// copy the group hash to the payload for use in resize
				memcpy(entry_payload_ptr, &group_hashes_ptr[index], hash_width);
				assert((*(hash_t *)entry_payload_ptr) == group_hashes_ptr[index]);

				// initialize the payload info for the column
				memcpy(entry_payload_ptr + hash_width + group_width, empty_payload_data.get(), payload_width);

				assert(group_hashes_ptr[index] >> hash_prefix_shift <= NumericLimits<uint16_t>::Maximum());
				assert(payload_block_idx < tuples_per_block);
				assert(payload_hds.size() < NumericLimits<uint16_t>::Maximum());
				assert(payload_block_idx + 1 < NumericLimits<uint32_t>::Maximum());

				ht_entry_ptr->salt = group_hashes_ptr[index] >> hash_prefix_shift;

				assert(((*(hash_t *)entry_payload_ptr) >> hash_prefix_shift) == ht_entry_ptr->salt);

				// page numbers start at one so we can use 0 as empty flag
				// GetPtr undoes this
				ht_entry_ptr->page_nr = payload_hds.size();
				ht_entry_ptr->page_offset = payload_block_idx++;

				// update selection lists for outer loops
				empty_vector.set_index(new_entry_count++, index);
				new_groups_out.set_index(new_group_count++, index);
				entries++;

			} else {
				// cell is occupied: add to check list
				// only need to check if hash salt in ptr == prefix of hash in payload
				if (((hash_t)ht_entry_ptr->salt << hash_prefix_shift) ==
				    (group_hashes_ptr[index] & hash_prefix_get_bitmask)) {
					group_compare_vector->set_index(need_compare_count++, index);
				} else {
					no_match_vector->set_index(no_match_count++, index);
				}
			}
			// keep pointers to each group area so we can scatter or compare them below
			group_pointers_ptr[index] = GetPtr(*ht_entry_ptr) + hash_width;
		}

		if (new_entry_count > 0) {
			// for each of the locations that are empty, serialize the group columns to the locations
			ScatterGroups(groups, group_data, group_pointers, empty_vector, new_entry_count);
		}
		// now we have only the tuples remaining that might match to an existing group
		// start performing comparisons with each of the groups
		if (need_compare_count > 0) {
			CompareGroups(groups, group_data, group_pointers, *group_compare_vector, need_compare_count,
			              *no_match_vector, no_match_count);
		}

		// each of the entries that do not match we move them to the next entry in the HT
		for (idx_t i = 0; i < no_match_count; i++) {
			idx_t index = no_match_vector->get_index(i);
			ht_offsets_ptr[index]++;
			if (ht_offsets_ptr[index] >= capacity) {
				ht_offsets_ptr[index] = 0;
			}
		}
		sel_vector = no_match_vector;
		remaining_entries = no_match_count;
	}

	assert(addresses_out.type == LogicalType::POINTER);

	// finally we create a bunch of pointers into the payload based on our offsets for callers to use
	addresses_out.Normalify(groups.size());

	UnaryExecutor::Execute<uint64_t, data_ptr_t>(ht_offsets, addresses_out, groups.size(), [&](uint64_t element) {
		auto ht_entry = ((aggr_ht_entry_64 *)this->hashes_hdl->Ptr()) + element;
		auto ret_ptr = GetPtr(*ht_entry);
		assert(((*(hash_t *)ret_ptr) >> hash_prefix_shift) == ht_entry->salt);
		return ret_ptr + hash_width + group_width;
	});

	return new_group_count;
}

void GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses) {
	// create a dummy new_groups sel vector
	SelectionVector new_groups(STANDARD_VECTOR_SIZE);
	FindOrCreateGroups(groups, addresses, new_groups);
}

idx_t GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses_out,
                                                    SelectionVector &new_groups_out) {
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);
	return FindOrCreateGroups(groups, hashes, addresses_out, new_groups_out);
}

void GroupedAggregateHashTable::FlushMerge(Vector &source_addresses, Vector &source_hashes, idx_t count) {
	assert(source_addresses.type == LogicalType::POINTER);
	assert(source_hashes.type == LogicalType::HASH);

	DataChunk groups;
	groups.Initialize(group_types);
	groups.SetCardinality(count);
	for (idx_t i = 0; i < groups.column_count(); i++) {
		auto &column = groups.data[i];
		VectorOperations::Gather::Set(source_addresses, column, groups.size());
	}

	SelectionVector new_groups(STANDARD_VECTOR_SIZE);
	Vector group_addresses(LogicalType::POINTER);
	SelectionVector new_groups_sel(STANDARD_VECTOR_SIZE);

	FindOrCreateGroups(groups, source_hashes, group_addresses, new_groups_sel);

	for (auto &aggr : aggregates) {
		// for any entries for which a group was found, update the aggregate
		assert(aggr.function.combine);
		aggr.function.combine(source_addresses, group_addresses, groups.size());
		VectorOperations::AddInPlace(source_addresses, aggr.payload_size, groups.size());
		VectorOperations::AddInPlace(group_addresses, aggr.payload_size, groups.size());
	}
}

void GroupedAggregateHashTable::Combine(GroupedAggregateHashTable &other) {
	if (finalized) {
		throw InternalException("HT already finalized");
	}
	assert(other.payload_width == payload_width);
	assert(other.group_width == group_width);
	assert(other.tuple_size == tuple_size);
	assert(other.tuples_per_block == tuples_per_block);

	if (other.entries == 0) {
		return;
	}
	Verify();
	other.Verify();

	Vector addresses(LogicalType::POINTER);
	auto addresses_ptr = FlatVector::GetData<data_ptr_t>(addresses);

	Vector hashes(LogicalType::HASH);
	auto hashes_ptr = FlatVector::GetData<hash_t>(hashes);

	idx_t group_idx = 0;
	idx_t merge_entries = other.entries;
	for (auto &payload_chunk : other.payload_hds) {
		auto this_entries = MinValue(tuples_per_block, merge_entries);
		for (data_ptr_t ptr = payload_chunk->Ptr(), end = payload_chunk->Ptr() + this_entries * tuple_size; ptr < end;
		     ptr += tuple_size) {
			hashes_ptr[group_idx] = *(hash_t *)ptr;
			addresses_ptr[group_idx] = ptr + hash_width;
			group_idx++;
			if (group_idx == STANDARD_VECTOR_SIZE) {
				FlushMerge(addresses, hashes, group_idx);
				group_idx = 0;
			}
		}
		merge_entries -= this_entries;
	}
	assert(merge_entries == 0);
	FlushMerge(addresses, hashes, group_idx);
	string_heap.MergeHeap(other.string_heap);
	// need to set the size to 0 since otherwise the destructors would be called and we took that stuff over
	other.entries = 0;

	Verify();
}

idx_t GroupedAggregateHashTable::Scan(idx_t &scan_position, DataChunk &groups, DataChunk &result) {
	if (!finalized) {
		throw InternalException("HT not finalized");
	}
	Vector addresses(LogicalType::POINTER);
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	auto remaining = entries - scan_position;
	if (remaining == 0) {
		return 0;
	}
	auto this_n = MinValue((idx_t)STANDARD_VECTOR_SIZE, remaining);

	auto chunk_idx = scan_position / tuples_per_block;
	auto chunk_offset = (scan_position % tuples_per_block) * tuple_size;
	assert(chunk_offset + tuple_size <= Storage::BLOCK_ALLOC_SIZE);

	auto read_ptr = payload_hds[chunk_idx++]->Ptr();
	for (idx_t i = 0; i < this_n; i++) {
		data_pointers[i] = read_ptr + chunk_offset + hash_width;
		chunk_offset += tuple_size;
		if (chunk_offset >= tuples_per_block * tuple_size) {
			read_ptr = payload_hds[chunk_idx++]->Ptr();
			chunk_offset = 0;
		}
	}

	groups.SetCardinality(this_n);
	result.SetCardinality(this_n);
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
	scan_position += this_n;
	return this_n;
}

void GroupedAggregateHashTable::Finalize() {
	// FIXME this breaks FetchAggregates hashes_hdl.reset();
	finalized = true;
}

} // namespace duckdb
