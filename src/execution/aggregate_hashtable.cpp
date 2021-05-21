#include "duckdb/execution/aggregate_hashtable.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cmath>
#include <map>

namespace duckdb {

using ValidityBytes = BaseAggregateHashTable::ValidityBytes;

GroupedAggregateHashTable::GroupedAggregateHashTable(BufferManager &buffer_manager, vector<LogicalType> group_types,
                                                     vector<LogicalType> payload_types,
                                                     const vector<BoundAggregateExpression *> &bindings,
                                                     HtEntryType entry_type)
    : GroupedAggregateHashTable(buffer_manager, move(group_types), move(payload_types),
                                AggregateObject::CreateAggregateObjects(bindings), entry_type) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(BufferManager &buffer_manager, vector<LogicalType> group_types)
    : GroupedAggregateHashTable(buffer_manager, move(group_types), {}, vector<AggregateObject>()) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(BufferManager &buffer_manager, vector<LogicalType> group_types_p,
                                                     vector<LogicalType> payload_types_p,
                                                     vector<AggregateObject> aggregate_objects_p,
                                                     HtEntryType entry_type)
    : BaseAggregateHashTable(buffer_manager, move(group_types_p), move(payload_types_p), move(aggregate_objects_p)),
      entry_type(entry_type), capacity(0), entries(0), payload_page_offset(0), is_finalized(false),
      ht_offsets(LogicalTypeId::BIGINT), hash_salts(LogicalTypeId::SMALLINT),
      group_compare_vector(STANDARD_VECTOR_SIZE), no_match_vector(STANDARD_VECTOR_SIZE),
      empty_vector(STANDARD_VECTOR_SIZE) {

	// HT layout
	tuple_size = HASH_WIDTH + layout.GetRowWidth();
#ifndef DUCKDB_ALLOW_UNDEFINED
	tuple_size = BaseAggregateHashTable::Align(tuple_size);
#endif

	D_ASSERT(tuple_size <= Storage::BLOCK_ALLOC_SIZE);
	tuples_per_block = Storage::BLOCK_ALLOC_SIZE / tuple_size;
	hashes_hdl = buffer_manager.Allocate(Storage::BLOCK_ALLOC_SIZE);
	hashes_hdl_ptr = hashes_hdl->Ptr();

	switch (entry_type) {
	case HtEntryType::HT_WIDTH_64: {
		hash_prefix_shift = (HASH_WIDTH - sizeof(aggr_ht_entry_64::salt)) * 8;
		Resize<aggr_ht_entry_64>(STANDARD_VECTOR_SIZE * 2);
		break;
	}
	case HtEntryType::HT_WIDTH_32: {
		hash_prefix_shift = (HASH_WIDTH - sizeof(aggr_ht_entry_32::salt)) * 8;
		Resize<aggr_ht_entry_32>(STANDARD_VECTOR_SIZE * 2);
		break;
	}
	default:
		throw NotImplementedException("Unknown HT entry width");
	}

	// create additional hash tables for distinct aggrs
	auto &aggregates = layout.GetAggregates();
	distinct_hashes.resize(aggregates.size());

	idx_t payload_idx = 0;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i];
		if (aggr.distinct) {
			// group types plus aggr return type
			vector<LogicalType> distinct_group_types(layout.GetTypes());
			for (idx_t child_idx = 0; child_idx < aggr.child_count; child_idx++) {
				distinct_group_types.push_back(payload_types[payload_idx]);
			}
			distinct_hashes[i] = make_unique<GroupedAggregateHashTable>(buffer_manager, distinct_group_types);
		}
		payload_idx += aggr.child_count;
	}
	addresses.Initialize(LogicalType::POINTER);
}

GroupedAggregateHashTable::~GroupedAggregateHashTable() {
	Destroy();
}

template <class FUNC>
void GroupedAggregateHashTable::PayloadApply(FUNC fun) {
	if (entries == 0) {
		return;
	}
	idx_t apply_entries = entries;
	idx_t page_nr = 0;
	idx_t page_offset = 0;

	for (auto &payload_chunk_ptr : payload_hds_ptrs) {
		auto this_entries = MinValue(tuples_per_block, apply_entries);
		page_offset = 0;
		for (data_ptr_t ptr = payload_chunk_ptr, end = payload_chunk_ptr + this_entries * tuple_size; ptr < end;
		     ptr += tuple_size) {
			fun(page_nr, page_offset++, ptr);
		}
		apply_entries -= this_entries;
		page_nr++;
	}
	D_ASSERT(apply_entries == 0);
}

void GroupedAggregateHashTable::NewBlock() {
	auto pin = buffer_manager.Allocate(Storage::BLOCK_ALLOC_SIZE);
	payload_hds.push_back(move(pin));
	payload_hds_ptrs.push_back(payload_hds.back()->Ptr());
	payload_page_offset = 0;
}

void GroupedAggregateHashTable::Destroy() {
	// check if there is a destructor
	bool has_destructor = false;
	for (auto &aggr : layout.GetAggregates()) {
		if (aggr.function.destructor) {
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

	PayloadApply([&](idx_t page_nr, idx_t page_offset, data_ptr_t ptr) {
		data_pointers[count++] = ptr + HASH_WIDTH + layout.GetAggrOffset();
		if (count == STANDARD_VECTOR_SIZE) {
			CallDestructors(state_vector, count);
			count = 0;
		}
	});
	CallDestructors(state_vector, count);
}

template <class T>
void GroupedAggregateHashTable::VerifyInternal() {
	auto hashes_ptr = (T *)hashes_hdl_ptr;
	D_ASSERT(payload_hds.size() == payload_hds_ptrs.size());
	idx_t count = 0;
	for (idx_t i = 0; i < capacity; i++) {
		if (hashes_ptr[i].page_nr > 0) {
			D_ASSERT(hashes_ptr[i].page_offset < tuples_per_block);
			D_ASSERT(hashes_ptr[i].page_nr <= payload_hds.size());
			auto ptr = payload_hds_ptrs[hashes_ptr[i].page_nr - 1] + ((hashes_ptr[i].page_offset) * tuple_size);
			auto hash = Load<hash_t>(ptr);
			D_ASSERT((hashes_ptr[i].salt) == (hash >> hash_prefix_shift));

			count++;
		}
	}
	D_ASSERT(count == entries);
}

idx_t GroupedAggregateHashTable::MaxCapacity() {
	idx_t max_pages = 0;
	idx_t max_tuples = 0;

	switch (entry_type) {
	case HtEntryType::HT_WIDTH_32:
		max_pages = NumericLimits<uint8_t>::Maximum();
		max_tuples = NumericLimits<uint16_t>::Maximum();
		break;
	default:
		D_ASSERT(entry_type == HtEntryType::HT_WIDTH_64);
		max_pages = NumericLimits<uint32_t>::Maximum();
		max_tuples = NumericLimits<uint16_t>::Maximum();
		break;
	}

	return max_pages * MinValue(max_tuples, (idx_t)Storage::BLOCK_ALLOC_SIZE / tuple_size);
}

void GroupedAggregateHashTable::Verify() {
#ifdef DEBUG
	switch (entry_type) {
	case HtEntryType::HT_WIDTH_32:
		VerifyInternal<aggr_ht_entry_32>();
		break;
	case HtEntryType::HT_WIDTH_64:
		VerifyInternal<aggr_ht_entry_64>();
		break;
	}
#endif
}

template <class T>
void GroupedAggregateHashTable::Resize(idx_t size) {
	Verify();

	D_ASSERT(!is_finalized);

	if (size <= capacity) {
		throw InternalException("Cannot downsize a hash table!");
	}
	if (size < STANDARD_VECTOR_SIZE) {
		size = STANDARD_VECTOR_SIZE;
	}

	// size needs to be a power of 2
	D_ASSERT((size & (size - 1)) == 0);
	bitmask = size - 1;

	auto byte_size = size * sizeof(T);
	if (byte_size > (idx_t)Storage::BLOCK_ALLOC_SIZE) {
		hashes_hdl = buffer_manager.Allocate(byte_size);
		hashes_hdl_ptr = hashes_hdl->Ptr();
	}
	memset(hashes_hdl_ptr, 0, byte_size);
	hashes_end_ptr = hashes_hdl_ptr + byte_size;
	capacity = size;

	auto hashes_arr = (T *)hashes_hdl_ptr;

	PayloadApply([&](idx_t page_nr, idx_t page_offset, data_ptr_t ptr) {
		auto hash = Load<hash_t>(ptr);
		D_ASSERT((hash & bitmask) == (hash % capacity));
		auto entry_idx = (idx_t)hash & bitmask;
		while (hashes_arr[entry_idx].page_nr > 0) {
			entry_idx++;
			if (entry_idx >= capacity) {
				entry_idx = 0;
			}
		}

		D_ASSERT(!hashes_arr[entry_idx].page_nr);
		D_ASSERT(hash >> hash_prefix_shift <= NumericLimits<uint16_t>::Maximum());

		hashes_arr[entry_idx].salt = hash >> hash_prefix_shift;
		hashes_arr[entry_idx].page_nr = page_nr + 1;
		hashes_arr[entry_idx].page_offset = page_offset;
	});

	Verify();
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload) {
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);

	return AddChunk(groups, hashes, payload);
}

void GroupedAggregateHashTable::UpdateAggregate(AggregateObject &aggr, DataChunk &payload, Vector &distinct_addresses,
                                                idx_t input_count, idx_t payload_idx) {
	ExpressionExecutor filter_execution(aggr.filter);
	SelectionVector true_sel(STANDARD_VECTOR_SIZE);
	auto count = filter_execution.SelectExpression(payload, true_sel);
	DataChunk filtered_payload;
	auto pay_types = payload.GetTypes();
	filtered_payload.Initialize(pay_types);
	filtered_payload.Slice(payload, true_sel, count);
	Vector filtered_addresses;
	filtered_addresses.Slice(distinct_addresses, true_sel, count);
	filtered_addresses.Normalify(count);
	aggr.function.update(input_count == 0 ? nullptr : &filtered_payload.data[payload_idx], nullptr, input_count,
	                     filtered_addresses, filtered_payload.size());
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, Vector &group_hashes, DataChunk &payload) {
	D_ASSERT(!is_finalized);

	if (groups.size() == 0) {
		return 0;
	}
	// dummy
	SelectionVector new_groups(STANDARD_VECTOR_SIZE);

	D_ASSERT(groups.ColumnCount() == layout.ColumnCount());
#ifdef DEBUG
	for (idx_t i = 0; i < layout.ColumnCount(); i++) {
		D_ASSERT(groups.GetTypes()[i] == layout.GetTypes()[i]);
	}
#endif

	Vector addresses(LogicalType::POINTER);
	auto new_group_count = FindOrCreateGroups(groups, group_hashes, addresses, new_groups);

	// now every cell has an entry
	// update the aggregates
	idx_t payload_idx = 0;

	auto &aggregates = layout.GetAggregates();
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		// for any entries for which a group was found, update the aggregate
		auto &aggr = aggregates[aggr_idx];
		auto input_count = (idx_t)aggr.child_count;
		if (aggr.distinct) {
			// construct chunk for secondary hash table probing
			vector<LogicalType> probe_types(layout.GetTypes());
			for (idx_t i = 0; i < aggr.child_count; i++) {
				probe_types.push_back(payload_types[payload_idx]);
			}
			DataChunk probe_chunk;
			probe_chunk.Initialize(probe_types);
			for (idx_t group_idx = 0; group_idx < layout.ColumnCount(); group_idx++) {
				probe_chunk.data[group_idx].Reference(groups.data[group_idx]);
			}
			for (idx_t i = 0; i < aggr.child_count; i++) {
				probe_chunk.data[layout.ColumnCount() + i].Reference(payload.data[payload_idx + i]);
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
				if (aggr.filter) {
					Vector distinct_addresses;
					DataChunk distinct_payload;
					distinct_addresses.Slice(addresses, new_groups, new_group_count);
					auto pay_types = payload.GetTypes();
					distinct_payload.Initialize(pay_types);
					distinct_payload.Slice(payload, new_groups, new_group_count);
					distinct_addresses.Verify(new_group_count);
					distinct_addresses.Normalify(new_group_count);
					UpdateAggregate(aggr, distinct_payload, distinct_addresses, input_count, payload_idx);
				} else {
					Vector distinct_addresses;
					distinct_addresses.Slice(addresses, new_groups, new_group_count);
					for (idx_t i = 0; i < aggr.child_count; i++) {
						payload.data[payload_idx + i].Slice(new_groups, new_group_count);
						payload.data[payload_idx + i].Verify(new_group_count);
					}
					distinct_addresses.Verify(new_group_count);

					aggr.function.update(input_count == 0 ? nullptr : &payload.data[payload_idx], nullptr, input_count,
					                     distinct_addresses, new_group_count);
				}
			}
		} else if (aggr.filter) {
			UpdateAggregate(aggr, payload, addresses, input_count, payload_idx);
		} else {
			aggr.function.update(input_count == 0 ? nullptr : &payload.data[payload_idx], aggr.bind_data, input_count,
			                     addresses, payload.size());
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
	D_ASSERT(groups.ColumnCount() == layout.ColumnCount());
	for (idx_t i = 0; i < result.ColumnCount(); i++) {
		D_ASSERT(result.data[i].GetType() == payload_types[i]);
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
	auto &aggregates = layout.GetAggregates();
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		D_ASSERT(result.ColumnCount() > aggr_idx);

		auto &target = result.data[aggr_idx];
		auto &aggr = aggregates[aggr_idx];
		aggr.function.finalize(addresses, aggr.bind_data, target, result.size());
		VectorOperations::AddInPlace(addresses, aggr.payload_size, result.size());
	}
}

template <class T>
static void TemplatedScatter(VectorData &gdata, Vector &addresses, const SelectionVector &sel, idx_t count,
                             idx_t col_offset, idx_t col_idx) {
	auto data = (T *)gdata.data;
	auto pointers = FlatVector::GetData<data_ptr_t>(addresses);

	if (!gdata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto pointer_idx = sel.get_index(i);
			auto group_idx = gdata.sel->get_index(pointer_idx);
			auto ptr = pointers[pointer_idx] + col_offset;

			auto isnull = !gdata.validity.RowIsValid(group_idx);
			T store_value = isnull ? NullValue<T>() : data[group_idx];
			Store<T>(store_value, ptr);
			if (isnull) {
				ValidityBytes col_mask(pointers[pointer_idx]);
				col_mask.SetInvalidUnsafe(col_idx);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto pointer_idx = sel.get_index(i);
			auto group_idx = gdata.sel->get_index(pointer_idx);
			auto ptr = pointers[pointer_idx] + col_offset;

			Store<T>(data[group_idx], ptr);
		}
	}
}

void GroupedAggregateHashTable::ScatterGroups(DataChunk &groups, unique_ptr<VectorData[]> &group_data,
                                              Vector &addresses, const SelectionVector &sel, idx_t count) {
	if (count == 0) {
		return;
	}
	auto &offsets = layout.GetOffsets();
	for (idx_t col_idx = 0; col_idx < groups.ColumnCount(); col_idx++) {
		auto &data = groups.data[col_idx];
		auto &gdata = group_data[col_idx];
		auto col_offset = offsets[col_idx];

		switch (data.GetType().InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedScatter<int8_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::INT16:
			TemplatedScatter<int16_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::INT32:
			TemplatedScatter<int32_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::INT64:
			TemplatedScatter<int64_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::UINT8:
			TemplatedScatter<uint8_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::UINT16:
			TemplatedScatter<uint16_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::UINT32:
			TemplatedScatter<uint32_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::UINT64:
			TemplatedScatter<uint64_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::INT128:
			TemplatedScatter<hugeint_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::FLOAT:
			TemplatedScatter<float>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::DOUBLE:
			TemplatedScatter<double>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::INTERVAL:
			TemplatedScatter<interval_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::VARCHAR: {
			auto string_data = (string_t *)gdata.data;
			auto pointers = FlatVector::GetData<data_ptr_t>(addresses);

			for (idx_t i = 0; i < count; i++) {
				auto pointer_idx = sel.get_index(i);
				auto group_idx = gdata.sel->get_index(pointer_idx);
				auto ptr = pointers[pointer_idx] + col_offset;
				if (!gdata.validity.RowIsValid(group_idx)) {
					ValidityBytes col_mask(pointers[pointer_idx]);
					col_mask.SetInvalidUnsafe(col_idx);
					Store<string_t>(NullValue<string_t>(), ptr);
				} else if (string_data[group_idx].IsInlined()) {
					Store<string_t>(string_data[group_idx], ptr);
				} else {
					Store<string_t>(
					    string_heap.AddBlob(string_data[group_idx].GetDataUnsafe(), string_data[group_idx].GetSize()),
					    ptr);
				}
			}
			break;
		}
		default:
			throw Exception("Unsupported type for group vector");
		}
		col_offset += GetTypeIdSize(data.GetType().InternalType());
	}
}

template <class T>
static void TemplatedCompareGroups(VectorData &gdata, Vector &addresses, SelectionVector &sel, idx_t &count,
                                   idx_t col_offset, idx_t col_idx, SelectionVector &no_match, idx_t &no_match_count) {
	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	auto data = (T *)gdata.data;
	auto pointers = FlatVector::GetData<data_ptr_t>(addresses);
	idx_t match_count = 0;
	if (!gdata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto group_idx = gdata.sel->get_index(idx);
			auto value = Load<T>(pointers[idx] + col_offset);
			ValidityBytes mask(pointers[idx]);
			auto isnull = !mask.RowIsValid(mask.GetValidityEntry(entry_idx), idx_in_entry);

			if (!gdata.validity.RowIsValid(group_idx)) {
				if (isnull) {
					// match: move to next value to compare
					sel.set_index(match_count++, idx);
				} else {
					no_match.set_index(no_match_count++, idx);
				}
			} else {
				if (!isnull && Equals::Operation<T>(data[group_idx], value)) {
					sel.set_index(match_count++, idx);
				} else {
					no_match.set_index(no_match_count++, idx);
				}
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto group_idx = gdata.sel->get_index(idx);
			auto value = Load<T>(pointers[idx] + col_offset);
			ValidityBytes mask(pointers[idx]);
			auto isnull = !mask.RowIsValid(mask.GetValidityEntry(entry_idx), idx_in_entry);

			if (!isnull && Equals::Operation<T>(data[group_idx], value)) {
				sel.set_index(match_count++, idx);
			} else {
				no_match.set_index(no_match_count++, idx);
			}
		}
	}
	count = match_count;
}

static void CompareGroups(DataChunk &groups, unique_ptr<VectorData[]> &group_data, Vector &addresses, idx_t mask_offset,
                          SelectionVector &sel, idx_t count, SelectionVector &no_match, idx_t &no_match_count) {
	auto col_offset = mask_offset;
	for (idx_t i = 0; i < groups.ColumnCount(); i++) {
		auto &data = groups.data[i];
		auto &gdata = group_data[i];
		switch (data.GetType().InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedCompareGroups<int8_t>(gdata, addresses, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::INT16:
			TemplatedCompareGroups<int16_t>(gdata, addresses, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::INT32:
			TemplatedCompareGroups<int32_t>(gdata, addresses, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::INT64:
			TemplatedCompareGroups<int64_t>(gdata, addresses, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::UINT8:
			TemplatedCompareGroups<uint8_t>(gdata, addresses, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::UINT16:
			TemplatedCompareGroups<uint16_t>(gdata, addresses, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::UINT32:
			TemplatedCompareGroups<uint32_t>(gdata, addresses, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::UINT64:
			TemplatedCompareGroups<uint64_t>(gdata, addresses, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::INT128:
			TemplatedCompareGroups<hugeint_t>(gdata, addresses, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::FLOAT:
			TemplatedCompareGroups<float>(gdata, addresses, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedCompareGroups<double>(gdata, addresses, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::INTERVAL:
			TemplatedCompareGroups<interval_t>(gdata, addresses, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::VARCHAR:
			TemplatedCompareGroups<string_t>(gdata, addresses, sel, count, col_offset, i, no_match, no_match_count);
			break;
		default:
			throw Exception("Unsupported type for group vector");
		}
		col_offset += GetTypeIdSize(data.GetType().InternalType());
	}
}

template <class T>
idx_t GroupedAggregateHashTable::FindOrCreateGroupsInternal(DataChunk &groups, Vector &group_hashes, Vector &addresses,
                                                            SelectionVector &new_groups_out) {

	D_ASSERT(!is_finalized);

	if (entries + groups.size() > MaxCapacity()) {
		throw InternalException("Hash table capacity reached");
	}

	// resize at 50% capacity, also need to fit the entire vector
	if (capacity - entries <= groups.size() || entries > capacity / LOAD_FACTOR) {
		Resize<T>(capacity * 2);
	}

	D_ASSERT(capacity - entries >= groups.size());
	D_ASSERT(groups.ColumnCount() == layout.ColumnCount());
	// we need to be able to fit at least one vector of data
	D_ASSERT(capacity - entries >= groups.size());
	D_ASSERT(group_hashes.GetType() == LogicalType::HASH);

	group_hashes.Normalify(groups.size());
	auto group_hashes_ptr = FlatVector::GetData<hash_t>(group_hashes);

	D_ASSERT(ht_offsets.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(ht_offsets.GetType() == LogicalType::BIGINT);

	D_ASSERT(addresses.GetType() == LogicalType::POINTER);
	addresses.Normalify(groups.size());
	auto addresses_ptr = FlatVector::GetData<data_ptr_t>(addresses);

	// now compute the entry in the table based on the hash using a modulo
	UnaryExecutor::Execute<hash_t, uint64_t>(group_hashes, ht_offsets, groups.size(), [&](hash_t element) {
		D_ASSERT((element & bitmask) == (element % capacity));
		return (element & bitmask);
	});
	auto ht_offsets_ptr = FlatVector::GetData<uint64_t>(ht_offsets);

	// precompute the hash salts for faster comparison below
	D_ASSERT(hash_salts.GetType() == LogicalType::SMALLINT);
	UnaryExecutor::Execute<hash_t, uint16_t>(group_hashes, hash_salts, groups.size(),
	                                         [&](hash_t element) { return (element >> hash_prefix_shift); });
	auto hash_salts_ptr = FlatVector::GetData<uint16_t>(hash_salts);

	// we start out with all entries [0, 1, 2, ..., groups.size()]
	const SelectionVector *sel_vector = &FlatVector::INCREMENTAL_SELECTION_VECTOR;

	idx_t remaining_entries = groups.size();

	// orrify all the groups
	auto group_data = unique_ptr<VectorData[]>(new VectorData[groups.ColumnCount()]);
	for (idx_t grp_idx = 0; grp_idx < groups.ColumnCount(); grp_idx++) {
		groups.data[grp_idx].Orrify(groups.size(), group_data[grp_idx]);
	}

	idx_t new_group_count = 0;
	while (remaining_entries > 0) {
		idx_t new_entry_count = 0;
		idx_t need_compare_count = 0;
		idx_t no_match_count = 0;

		// first figure out for each remaining whether or not it belongs to a full or empty group
		for (idx_t i = 0; i < remaining_entries; i++) {
			const idx_t index = sel_vector->get_index(i);
			const auto ht_entry_ptr = ((T *)this->hashes_hdl_ptr) + ht_offsets_ptr[index];
			if (ht_entry_ptr->page_nr == 0) { // we use page number 0 as a "unused marker"
				// cell is empty; setup the new entry
				if (payload_page_offset == tuples_per_block || payload_hds.empty()) {
					NewBlock();
				}

				auto entry_payload_ptr = payload_hds_ptrs.back() + (payload_page_offset * tuple_size);

				// copy the group hash to the payload for use in resize
				memcpy(entry_payload_ptr, &group_hashes_ptr[index], HASH_WIDTH);
				D_ASSERT((*(hash_t *)entry_payload_ptr) == group_hashes_ptr[index]);

				// Set the validity mask (clearing is less common)
				ValidityBytes(entry_payload_ptr + HASH_WIDTH).SetAllValid(layout.ColumnCount());

				// initialize the payload info for the column
				memcpy(entry_payload_ptr + HASH_WIDTH + layout.GetAggrOffset(), empty_payload_data.get(),
				       layout.GetAggrWidth());

				D_ASSERT(group_hashes_ptr[index] >> hash_prefix_shift <= NumericLimits<uint16_t>::Maximum());
				D_ASSERT(payload_page_offset < tuples_per_block);
				D_ASSERT(payload_hds.size() < NumericLimits<uint32_t>::Maximum());
				D_ASSERT(payload_page_offset + 1 < NumericLimits<uint16_t>::Maximum());

				ht_entry_ptr->salt = group_hashes_ptr[index] >> hash_prefix_shift;

				D_ASSERT(((*(hash_t *)entry_payload_ptr) >> hash_prefix_shift) == ht_entry_ptr->salt);

				// page numbers start at one so we can use 0 as empty flag
				// GetPtr undoes this
				ht_entry_ptr->page_nr = payload_hds.size();
				ht_entry_ptr->page_offset = payload_page_offset++;

				// update selection lists for outer loops
				empty_vector.set_index(new_entry_count++, index);
				new_groups_out.set_index(new_group_count++, index);
				entries++;

				addresses_ptr[index] = entry_payload_ptr + HASH_WIDTH;

			} else {
				// cell is occupied: add to check list
				// only need to check if hash salt in ptr == prefix of hash in payload
				if (ht_entry_ptr->salt == hash_salts_ptr[index]) {
					group_compare_vector.set_index(need_compare_count++, index);

					auto page_ptr = payload_hds_ptrs[ht_entry_ptr->page_nr - 1];
					auto page_offset = ht_entry_ptr->page_offset * tuple_size;
					addresses_ptr[index] = page_ptr + page_offset + HASH_WIDTH;

				} else {
					no_match_vector.set_index(no_match_count++, index);
				}
			}
		}

		if (new_entry_count > 0) {
			// for each of the locations that are empty, serialize the group columns to the locations
			ScatterGroups(groups, group_data, addresses, empty_vector, new_entry_count);
		}
		// now we have only the tuples remaining that might match to an existing group
		// start performing comparisons with each of the groups
		if (need_compare_count > 0) {
			CompareGroups(groups, group_data, addresses, layout.GetDataOffset(), group_compare_vector,
			              need_compare_count, no_match_vector, no_match_count);
		}

		// each of the entries that do not match we move them to the next entry in the HT
		for (idx_t i = 0; i < no_match_count; i++) {
			idx_t index = no_match_vector.get_index(i);
			ht_offsets_ptr[index]++;
			if (ht_offsets_ptr[index] >= capacity) {
				ht_offsets_ptr[index] = 0;
			}
		}
		sel_vector = &no_match_vector;
		remaining_entries = no_match_count;
	}
	// pointers in addresses now were moved behind the groups by CompareGroups/ScatterGroups but we may have to add
	// padding still to point at the payload.
	VectorOperations::AddInPlace(addresses, layout.GetAggrOffset(), groups.size());
	return new_group_count;
}

// this is to support distinct aggregations where we need to record whether we
// have already seen a value for a group
idx_t GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &group_hashes, Vector &addresses_out,
                                                    SelectionVector &new_groups_out) {
	switch (entry_type) {
	case HtEntryType::HT_WIDTH_64:
		return FindOrCreateGroupsInternal<aggr_ht_entry_64>(groups, group_hashes, addresses_out, new_groups_out);
	case HtEntryType::HT_WIDTH_32:
		return FindOrCreateGroupsInternal<aggr_ht_entry_32>(groups, group_hashes, addresses_out, new_groups_out);
	default:
		throw NotImplementedException("Unknown HT entry width");
	}
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

void GroupedAggregateHashTable::FlushMove(Vector &source_addresses, Vector &source_hashes, idx_t count) {
	D_ASSERT(source_addresses.GetType() == LogicalType::POINTER);
	D_ASSERT(source_hashes.GetType() == LogicalType::HASH);

	DataChunk groups;
	groups.Initialize(layout.GetTypes());
	groups.SetCardinality(count);
	const auto &offsets = layout.GetOffsets();
	for (idx_t i = 0; i < groups.ColumnCount(); i++) {
		auto &column = groups.data[i];
		VectorOperations::Gather::Set(source_addresses, column, count, offsets[i], i);
	}
	VectorOperations::AddInPlace(source_addresses, layout.GetAggrOffset(), count);

	SelectionVector new_groups(STANDARD_VECTOR_SIZE);
	Vector group_addresses(LogicalType::POINTER);
	SelectionVector new_groups_sel(STANDARD_VECTOR_SIZE);

	FindOrCreateGroups(groups, source_hashes, group_addresses, new_groups_sel);

	for (auto &aggr : layout.GetAggregates()) {
		// for any entries for which a group was found, update the aggregate
		D_ASSERT(aggr.function.combine);
		aggr.function.combine(source_addresses, group_addresses, count);
		VectorOperations::AddInPlace(source_addresses, aggr.payload_size, count);
		VectorOperations::AddInPlace(group_addresses, aggr.payload_size, count);
	}
}

void GroupedAggregateHashTable::Combine(GroupedAggregateHashTable &other) {

	D_ASSERT(!is_finalized);

	D_ASSERT(other.layout.GetAggrWidth() == layout.GetAggrWidth());
	D_ASSERT(other.layout.GetDataWidth() == layout.GetDataWidth());
	D_ASSERT(other.layout.GetRowWidth() == layout.GetRowWidth());
	D_ASSERT(other.tuples_per_block == tuples_per_block);

	if (other.entries == 0) {
		return;
	}

	Vector addresses(LogicalType::POINTER);
	auto addresses_ptr = FlatVector::GetData<data_ptr_t>(addresses);

	Vector hashes(LogicalType::HASH);
	auto hashes_ptr = FlatVector::GetData<hash_t>(hashes);

	idx_t group_idx = 0;

	other.PayloadApply([&](idx_t page_nr, idx_t page_offset, data_ptr_t ptr) {
		auto hash = Load<hash_t>(ptr);

		hashes_ptr[group_idx] = hash;
		addresses_ptr[group_idx] = ptr + HASH_WIDTH;
		group_idx++;
		if (group_idx == STANDARD_VECTOR_SIZE) {
			FlushMove(addresses, hashes, group_idx);
			group_idx = 0;
		}
	});
	FlushMove(addresses, hashes, group_idx);
	string_heap.MergeHeap(other.string_heap);
	Verify();
}

struct PartitionInfo {
	PartitionInfo() : addresses(LogicalType::POINTER), hashes(LogicalType::HASH), group_count(0) {
		addresses_ptr = FlatVector::GetData<data_ptr_t>(addresses);
		hashes_ptr = FlatVector::GetData<hash_t>(hashes);
	};
	Vector addresses;
	Vector hashes;
	idx_t group_count;
	data_ptr_t *addresses_ptr;
	hash_t *hashes_ptr;
};

void GroupedAggregateHashTable::Partition(vector<GroupedAggregateHashTable *> &partition_hts, hash_t mask,
                                          idx_t shift) {
	D_ASSERT(partition_hts.size() > 1);
	vector<PartitionInfo> partition_info(partition_hts.size());

	PayloadApply([&](idx_t page_nr, idx_t page_offset, data_ptr_t ptr) {
		auto hash = Load<hash_t>(ptr);

		idx_t partition = (hash & mask) >> shift;
		D_ASSERT(partition < partition_hts.size());

		auto &info = partition_info[partition];

		info.hashes_ptr[info.group_count] = hash;
		info.addresses_ptr[info.group_count] = ptr + HASH_WIDTH;
		info.group_count++;
		if (info.group_count == STANDARD_VECTOR_SIZE) {
			D_ASSERT(partition_hts[partition]);
			partition_hts[partition]->FlushMove(info.addresses, info.hashes, info.group_count);
			info.group_count = 0;
		}
	});

	idx_t info_idx = 0;
	idx_t total_count = 0;
	for (auto &partition_entry : partition_hts) {
		auto &info = partition_info[info_idx++];
		partition_entry->FlushMove(info.addresses, info.hashes, info.group_count);

		partition_entry->string_heap.MergeHeap(string_heap);
		partition_entry->Verify();
		total_count += partition_entry->Size();
	}
	D_ASSERT(total_count == entries);
}

idx_t GroupedAggregateHashTable::Scan(idx_t &scan_position, DataChunk &result) {
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	auto remaining = entries - scan_position;
	if (remaining == 0) {
		return 0;
	}
	auto this_n = MinValue((idx_t)STANDARD_VECTOR_SIZE, remaining);

	auto chunk_idx = scan_position / tuples_per_block;
	auto chunk_offset = (scan_position % tuples_per_block) * tuple_size;
	D_ASSERT(chunk_offset + tuple_size <= Storage::BLOCK_ALLOC_SIZE);

	auto read_ptr = payload_hds_ptrs[chunk_idx++];
	for (idx_t i = 0; i < this_n; i++) {
		data_pointers[i] = read_ptr + chunk_offset + HASH_WIDTH;
		chunk_offset += tuple_size;
		if (chunk_offset >= tuples_per_block * tuple_size) {
			read_ptr = payload_hds_ptrs[chunk_idx++];
			chunk_offset = 0;
		}
	}

	result.SetCardinality(this_n);
	// fetch the group columns
	const auto &offsets = layout.GetOffsets();
	for (idx_t i = 0; i < layout.ColumnCount(); i++) {
		auto &column = result.data[i];
		VectorOperations::Gather::Set(addresses, column, result.size(), offsets[i], i);
	}
	VectorOperations::AddInPlace(addresses, layout.GetAggrOffset(), result.size());

	auto &aggregates = layout.GetAggregates();
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &target = result.data[layout.ColumnCount() + i];
		auto &aggr = aggregates[i];
		aggr.function.finalize(addresses, aggr.bind_data, target, result.size());
		VectorOperations::AddInPlace(addresses, aggr.payload_size, result.size());
	}
	scan_position += this_n;
	return this_n;
}

void GroupedAggregateHashTable::Finalize() {
	D_ASSERT(!is_finalized);

	// early release hashes, not needed for partition/scan
	hashes_hdl.reset();
	is_finalized = true;
}

} // namespace duckdb
