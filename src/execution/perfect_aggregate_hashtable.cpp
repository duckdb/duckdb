#include "duckdb/execution/perfect_aggregate_hashtable.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"

namespace duckdb {

PerfectAggregateHashTable::PerfectAggregateHashTable(Allocator &allocator, BufferManager &buffer_manager,
                                                     const vector<LogicalType> &group_types_p,
                                                     vector<LogicalType> payload_types_p,
                                                     vector<AggregateObject> aggregate_objects_p,
                                                     vector<Value> group_minima_p, vector<idx_t> required_bits_p)
    : BaseAggregateHashTable(allocator, aggregate_objects_p, buffer_manager, move(payload_types_p)),
      addresses(LogicalType::POINTER), required_bits(move(required_bits_p)), total_required_bits(0),
      group_minima(move(group_minima_p)), sel(STANDARD_VECTOR_SIZE) {
	for (auto &group_bits : required_bits) {
		total_required_bits += group_bits;
	}
	// the total amount of groups we allocate space for is 2^required_bits
	total_groups = 1 << total_required_bits;
	// we don't need to store the groups in a perfect hash table, since the group keys can be deduced by their location
	grouping_columns = group_types_p.size();
	layout.Initialize(move(aggregate_objects_p));
	tuple_size = layout.GetRowWidth();

	// allocate and null initialize the data
	owned_data = unique_ptr<data_t[]>(new data_t[tuple_size * total_groups]);
	data = owned_data.get();

	// set up the empty payloads for every tuple, and initialize the "occupied" flag to false
	group_is_set = unique_ptr<bool[]>(new bool[total_groups]);
	memset(group_is_set.get(), 0, total_groups * sizeof(bool));
}

PerfectAggregateHashTable::~PerfectAggregateHashTable() {
	Destroy();
}

template <class T>
static void ComputeGroupLocationTemplated(UnifiedVectorFormat &group_data, Value &min, uintptr_t *address_data,
                                          idx_t current_shift, idx_t count) {
	auto data = (T *)group_data.data;
	auto min_val = min.GetValueUnsafe<T>();
	if (!group_data.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto index = group_data.sel->get_index(i);
			// check if the value is NULL
			// NULL groups are considered as "0" in the hash table
			// that is to say, they have no effect on the position of the element (because 0 << shift is 0)
			// we only need to handle non-null values here
			if (group_data.validity.RowIsValid(index)) {
				D_ASSERT(data[index] >= min_val);
				uintptr_t adjusted_value = (data[index] - min_val) + 1;
				address_data[i] += adjusted_value << current_shift;
			}
		}
	} else {
		// no null values: we can directly compute the addresses
		for (idx_t i = 0; i < count; i++) {
			auto index = group_data.sel->get_index(i);
			uintptr_t adjusted_value = (data[index] - min_val) + 1;
			address_data[i] += adjusted_value << current_shift;
		}
	}
}

static void ComputeGroupLocation(Vector &group, Value &min, uintptr_t *address_data, idx_t current_shift, idx_t count) {
	UnifiedVectorFormat vdata;
	group.ToUnifiedFormat(count, vdata);

	switch (group.GetType().InternalType()) {
	case PhysicalType::INT8:
		ComputeGroupLocationTemplated<int8_t>(vdata, min, address_data, current_shift, count);
		break;
	case PhysicalType::INT16:
		ComputeGroupLocationTemplated<int16_t>(vdata, min, address_data, current_shift, count);
		break;
	case PhysicalType::INT32:
		ComputeGroupLocationTemplated<int32_t>(vdata, min, address_data, current_shift, count);
		break;
	case PhysicalType::INT64:
		ComputeGroupLocationTemplated<int64_t>(vdata, min, address_data, current_shift, count);
		break;
	default:
		throw InternalException("Unsupported group type for perfect aggregate hash table");
	}
}

void PerfectAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload) {
	// first we need to find the location in the HT of each of the groups
	auto address_data = FlatVector::GetData<uintptr_t>(addresses);
	// zero-initialize the address data
	memset(address_data, 0, groups.size() * sizeof(uintptr_t));
	D_ASSERT(groups.ColumnCount() == group_minima.size());

	// then compute the actual group location by iterating over each of the groups
	idx_t current_shift = total_required_bits;
	for (idx_t i = 0; i < groups.ColumnCount(); i++) {
		current_shift -= required_bits[i];
		ComputeGroupLocation(groups.data[i], group_minima[i], address_data, current_shift, groups.size());
	}
	// now we have the HT entry number for every tuple
	// compute the actual pointer to the data by adding it to the base HT pointer and multiplying by the tuple size
	idx_t needs_init = 0;
	for (idx_t i = 0; i < groups.size(); i++) {
		D_ASSERT(address_data[i] < total_groups);
		const auto group = address_data[i];
		address_data[i] = uintptr_t(data) + address_data[i] * tuple_size;
		if (!group_is_set[group]) {
			group_is_set[group] = true;
			sel.set_index(needs_init++, i);
			if (needs_init == STANDARD_VECTOR_SIZE) {
				RowOperations::InitializeStates(layout, addresses, sel, needs_init);
				needs_init = 0;
			}
		}
	}
	RowOperations::InitializeStates(layout, addresses, sel, needs_init);

	// after finding the group location we update the aggregates
	idx_t payload_idx = 0;
	auto &aggregates = layout.GetAggregates();
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = aggregates[aggr_idx];
		auto input_count = (idx_t)aggregate.child_count;
		if (aggregate.filter) {
			RowOperations::UpdateFilteredStates(filter_set.GetFilterData(aggr_idx), aggregate, addresses, payload,
			                                    payload_idx);
		} else {
			RowOperations::UpdateStates(aggregate, addresses, payload, payload_idx, payload.size());
		}
		// move to the next aggregate
		payload_idx += input_count;
		VectorOperations::AddInPlace(addresses, aggregate.payload_size, payload.size());
	}
}

void PerfectAggregateHashTable::Combine(PerfectAggregateHashTable &other) {
	D_ASSERT(total_groups == other.total_groups);
	D_ASSERT(tuple_size == other.tuple_size);

	Vector source_addresses(LogicalType::POINTER);
	Vector target_addresses(LogicalType::POINTER);
	auto source_addresses_ptr = FlatVector::GetData<data_ptr_t>(source_addresses);
	auto target_addresses_ptr = FlatVector::GetData<data_ptr_t>(target_addresses);

	// iterate over all entries of both hash tables and call combine for all entries that can be combined
	data_ptr_t source_ptr = other.data;
	data_ptr_t target_ptr = data;
	idx_t combine_count = 0;
	idx_t reinit_count = 0;
	const auto &reinit_sel = *FlatVector::IncrementalSelectionVector();
	for (idx_t i = 0; i < total_groups; i++) {
		auto has_entry_source = other.group_is_set[i];
		// we only have any work to do if the source has an entry for this group
		if (has_entry_source) {
			auto has_entry_target = group_is_set[i];
			if (has_entry_target) {
				// both source and target have an entry: need to combine
				source_addresses_ptr[combine_count] = source_ptr;
				target_addresses_ptr[combine_count] = target_ptr;
				combine_count++;
				if (combine_count == STANDARD_VECTOR_SIZE) {
					RowOperations::CombineStates(layout, source_addresses, target_addresses, combine_count);
					combine_count = 0;
				}
			} else {
				group_is_set[i] = true;
				// only source has an entry for this group: we can just memcpy it over
				memcpy(target_ptr, source_ptr, tuple_size);
				// we clear this entry in the other HT as we "consume" the entry here
				other.group_is_set[i] = false;
			}
		}
		source_ptr += tuple_size;
		target_ptr += tuple_size;
	}
	RowOperations::CombineStates(layout, source_addresses, target_addresses, combine_count);
	RowOperations::InitializeStates(layout, addresses, reinit_sel, reinit_count);
}

template <class T>
static void ReconstructGroupVectorTemplated(uint32_t group_values[], Value &min, idx_t mask, idx_t shift,
                                            idx_t entry_count, Vector &result) {
	auto data = FlatVector::GetData<T>(result);
	auto &validity_mask = FlatVector::Validity(result);
	auto min_data = min.GetValueUnsafe<T>();
	for (idx_t i = 0; i < entry_count; i++) {
		// extract the value of this group from the total group index
		auto group_index = (group_values[i] >> shift) & mask;
		if (group_index == 0) {
			// if it is 0, the value is NULL
			validity_mask.SetInvalid(i);
		} else {
			// otherwise we add the value (minus 1) to the min value
			data[i] = min_data + group_index - 1;
		}
	}
}

static void ReconstructGroupVector(uint32_t group_values[], Value &min, idx_t required_bits, idx_t shift,
                                   idx_t entry_count, Vector &result) {
	// construct the mask for this entry
	idx_t mask = (1 << required_bits) - 1;
	switch (result.GetType().InternalType()) {
	case PhysicalType::INT8:
		ReconstructGroupVectorTemplated<int8_t>(group_values, min, mask, shift, entry_count, result);
		break;
	case PhysicalType::INT16:
		ReconstructGroupVectorTemplated<int16_t>(group_values, min, mask, shift, entry_count, result);
		break;
	case PhysicalType::INT32:
		ReconstructGroupVectorTemplated<int32_t>(group_values, min, mask, shift, entry_count, result);
		break;
	case PhysicalType::INT64:
		ReconstructGroupVectorTemplated<int64_t>(group_values, min, mask, shift, entry_count, result);
		break;
	default:
		throw InternalException("Invalid type for perfect aggregate HT group");
	}
}

void PerfectAggregateHashTable::Scan(idx_t &scan_position, DataChunk &result) {
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);
	uint32_t group_values[STANDARD_VECTOR_SIZE];

	// iterate over the HT until we either have exhausted the entire HT, or
	idx_t entry_count = 0;
	for (; scan_position < total_groups; scan_position++) {
		if (group_is_set[scan_position]) {
			// this group is set: add it to the set of groups to extract
			data_pointers[entry_count] = data + tuple_size * scan_position;
			group_values[entry_count] = scan_position;
			entry_count++;
			if (entry_count == STANDARD_VECTOR_SIZE) {
				scan_position++;
				break;
			}
		}
	}
	if (entry_count == 0) {
		// no entries found
		return;
	}
	// first reconstruct the groups from the group index
	idx_t shift = total_required_bits;
	for (idx_t i = 0; i < grouping_columns; i++) {
		shift -= required_bits[i];
		ReconstructGroupVector(group_values, group_minima[i], required_bits[i], shift, entry_count, result.data[i]);
	}
	// then construct the payloads
	result.SetCardinality(entry_count);
	RowOperations::FinalizeStates(layout, addresses, result, grouping_columns);
}

void PerfectAggregateHashTable::Destroy() {
	// check if there is any destructor to call
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
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);
	idx_t count = 0;

	// iterate over all initialised slots of the hash table
	data_ptr_t payload_ptr = data;
	for (idx_t i = 0; i < total_groups; i++) {
		if (group_is_set[i]) {
			data_pointers[count++] = payload_ptr;
			if (count == STANDARD_VECTOR_SIZE) {
				RowOperations::DestroyStates(layout, addresses, count);
				count = 0;
			}
		}
		payload_ptr += tuple_size;
	}
	RowOperations::DestroyStates(layout, addresses, count);
}

} // namespace duckdb
