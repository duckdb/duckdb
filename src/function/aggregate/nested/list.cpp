#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

struct ListSegment {
	uint16_t count;
	uint16_t capacity;
	ListSegment *next;
};

struct LinkedList {
	idx_t total_capacity = 0;
	ListSegment *first_segment = nullptr;
	ListSegment *last_segment = nullptr;
};

// allocate the header size, the size of the null_mask and the data size (capacity * T)
/*
    #3 -> {
        uint16_t count = 4;
        uint16_t capacity = 4;
        list_vector_data_t *next = #4;
        bool is_null[4] = { false, false, false, false };
        int32_t values[4] = { 1, 2, 3, 4 }
        }
 */
template <class T>
void *AllocatePrimitiveData(uint16_t capacity) {
	return malloc(sizeof(ListSegment) + capacity * (sizeof(bool) + sizeof(T)));
}

template <class T>
T *TemplatedGetPrimitiveData(ListSegment *segment) {
	return (T *)(((char *)segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

void SetPrimitiveDataValue(ListSegment *segment, const LogicalType &type, data_ptr_t &input_data, idx_t &row_idx) {

	auto physical_type = type.InternalType();
	switch (physical_type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL: {
		auto data = TemplatedGetPrimitiveData<bool>(segment);
		data[segment->count] = ((bool *)input_data)[row_idx];
		break;
	}
	case PhysicalType::INT8: {
		auto data = TemplatedGetPrimitiveData<int8_t>(segment);
		data[segment->count] = ((int8_t *)input_data)[row_idx];
		break;
	}
	case PhysicalType::INT16: {
		auto data = TemplatedGetPrimitiveData<int16_t>(segment);
		data[segment->count] = ((int16_t *)input_data)[row_idx];
		break;
	}
	case PhysicalType::INT32: {
		auto data = TemplatedGetPrimitiveData<int32_t>(segment);
		data[segment->count] = ((int32_t *)input_data)[row_idx];
		break;
	}
	case PhysicalType::INT64: {
		auto data = TemplatedGetPrimitiveData<int64_t>(segment);
		data[segment->count] = ((int64_t *)input_data)[row_idx];
		break;
	}
	case PhysicalType::UINT8: {
		auto data = TemplatedGetPrimitiveData<uint8_t>(segment);
		data[segment->count] = ((uint8_t *)input_data)[row_idx];
		break;
	}
	case PhysicalType::UINT16: {
		auto data = TemplatedGetPrimitiveData<uint16_t>(segment);
		data[segment->count] = ((uint16_t *)input_data)[row_idx];
		break;
	}
	case PhysicalType::UINT32: {
		auto data = TemplatedGetPrimitiveData<uint32_t>(segment);
		data[segment->count] = ((uint32_t *)input_data)[row_idx];
		break;
	}
	case PhysicalType::UINT64: {
		auto data = TemplatedGetPrimitiveData<uint64_t>(segment);
		data[segment->count] = ((uint64_t *)input_data)[row_idx];
		break;
	}
	case PhysicalType::FLOAT: {
		auto data = TemplatedGetPrimitiveData<float>(segment);
		data[segment->count] = ((float *)input_data)[row_idx];
		break;
	}
	case PhysicalType::DOUBLE: {
		auto data = TemplatedGetPrimitiveData<double>(segment);
		data[segment->count] = ((double *)input_data)[row_idx];
		break;
	}
	case PhysicalType::VARCHAR:
		// string_t
		// TODO: store string differently
		throw InternalException("LIST aggregate not yet implemented for strings!");
	default:
		// INT128, INTERVAL, STRUCT, LIST
		throw InternalException("LIST aggregate not yet implemented for " + TypeIdToString(type.InternalType()));
	}
}

void GetPrimitiveDataValue(ListSegment *segment, const LogicalType &type, data_ptr_t &vector_data, idx_t &segment_idx,
                           idx_t row_idx) {

	auto physical_type = type.InternalType();
	switch (physical_type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL: {
		auto data = TemplatedGetPrimitiveData<bool>(segment);
		((bool *)vector_data)[row_idx] = data[segment_idx];
		break;
	}
	case PhysicalType::INT8: {
		auto data = TemplatedGetPrimitiveData<int8_t>(segment);
		((int8_t *)vector_data)[row_idx] = data[segment_idx];
		break;
	}
	case PhysicalType::INT16: {
		auto data = TemplatedGetPrimitiveData<int16_t>(segment);
		((int16_t *)vector_data)[row_idx] = data[segment_idx];
		break;
	}
	case PhysicalType::INT32: {
		auto data = TemplatedGetPrimitiveData<int32_t>(segment);
		((int32_t *)vector_data)[row_idx] = data[segment_idx];
		break;
	}
	case PhysicalType::INT64: {
		auto data = TemplatedGetPrimitiveData<int64_t>(segment);
		((int64_t *)vector_data)[row_idx] = data[segment_idx];
		break;
	}
	case PhysicalType::UINT8: {
		auto data = TemplatedGetPrimitiveData<uint8_t>(segment);
		((uint8_t *)vector_data)[row_idx] = data[segment_idx];
		break;
	}
	case PhysicalType::UINT16: {
		auto data = TemplatedGetPrimitiveData<uint16_t>(segment);
		((uint16_t *)vector_data)[row_idx] = data[segment_idx];
		break;
	}
	case PhysicalType::UINT32: {
		auto data = TemplatedGetPrimitiveData<uint32_t>(segment);
		((uint32_t *)vector_data)[row_idx] = data[segment_idx];
		break;
	}
	case PhysicalType::UINT64: {
		auto data = TemplatedGetPrimitiveData<uint64_t>(segment);
		((uint64_t *)vector_data)[row_idx] = data[segment_idx];
		break;
	}
	case PhysicalType::FLOAT: {
		auto data = TemplatedGetPrimitiveData<float>(segment);
		((float *)vector_data)[row_idx] = data[segment_idx];
		break;
	}
	case PhysicalType::DOUBLE: {
		auto data = TemplatedGetPrimitiveData<double>(segment);
		((double *)vector_data)[row_idx] = data[segment_idx];
		break;
	}
	case PhysicalType::VARCHAR:
		// string_t
		// TODO: store string differently
		throw InternalException("LIST aggregate not yet implemented for strings!");
	default:
		// INT128, INTERVAL, STRUCT, LIST
		throw InternalException("LIST aggregate not yet implemented for " + TypeIdToString(type.InternalType()));
	}
}

// allocate the header size, the size of the null_mask, the size of the list offsets (capacity * list_entry_t)
// and the linked list of the child entries
/*
    #1 -> {
        uint16_t count = 4;
        uint16_t capacity = 4;
        list_vector_data_t *next = NULL;
        bool is_null[4] = { false, false, true, false };
        list_entry_t list_offsets[4] = { {0, 3}, {3, 2}, _, {5, 4} };
        linked_list_t child { first = #3, last = #4 };
    }
 */
void *AllocateListData(uint16_t capacity) {
	return malloc(sizeof(ListSegment) + capacity * (sizeof(bool) + sizeof(list_entry_t)) + sizeof(LinkedList));
}

list_entry_t *GetListOffsetData(ListSegment *segment) {
	return (list_entry_t *)(((char *)segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

LinkedList *GetListChildData(ListSegment *segment) {
	return (LinkedList *)(((char *)segment) + sizeof(ListSegment) +
	                      segment->capacity * (sizeof(bool) + sizeof(list_entry_t)));
}

// allocate the header size, the size of the null_mask and the size of the children pointers
/*
    #0 -> {
        uint16_t count = 4;
        uint16_t capacity = 4;
        list_vector_data_t *next = NULL;
        bool is_null[4] = { false, false, true, false };
        list_vector_data_t *children[2] = { #1, #2 };
    };
 */
void *AllocateStructData(uint16_t capacity, idx_t child_count) {
	return malloc(sizeof(ListSegment) + capacity * sizeof(bool) + child_count * sizeof(ListSegment *));
}

ListSegment *GetStructData(ListSegment *segment) {
	return (ListSegment *)(((char *)segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

// this is the same for all segment types
bool *GetNullMask(ListSegment *segment) {
	return (bool *)(((char *)segment) + sizeof(ListSegment));
}

template <class T>
ListSegment *TemplatedCreatePrimitiveSegment(LinkedList *linked_list) {

	uint16_t capacity = 4;
	if (linked_list->last_segment) {
		auto next_power_of_two = linked_list->last_segment->capacity * 2;
		capacity = next_power_of_two < 65536 ? next_power_of_two : linked_list->last_segment->capacity;
	}
	auto segment = (ListSegment *)AllocatePrimitiveData<T>(capacity);
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;
	return segment;
}

ListSegment *CreatePrimitiveSegment(LinkedList *linked_list, const LogicalType &type) {

	auto physical_type = type.InternalType();

	switch (physical_type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL:
		return TemplatedCreatePrimitiveSegment<bool>(linked_list);
	case PhysicalType::INT8:
		return TemplatedCreatePrimitiveSegment<int8_t>(linked_list);
	case PhysicalType::INT16:
		return TemplatedCreatePrimitiveSegment<int16_t>(linked_list);
	case PhysicalType::INT32:
		return TemplatedCreatePrimitiveSegment<int32_t>(linked_list);
	case PhysicalType::INT64:
		return TemplatedCreatePrimitiveSegment<int64_t>(linked_list);
	case PhysicalType::UINT8:
		return TemplatedCreatePrimitiveSegment<uint8_t>(linked_list);
	case PhysicalType::UINT16:
		return TemplatedCreatePrimitiveSegment<uint16_t>(linked_list);
	case PhysicalType::UINT32:
		return TemplatedCreatePrimitiveSegment<uint32_t>(linked_list);
	case PhysicalType::UINT64:
		return TemplatedCreatePrimitiveSegment<uint64_t>(linked_list);
	case PhysicalType::FLOAT:
		return TemplatedCreatePrimitiveSegment<float>(linked_list);
	case PhysicalType::DOUBLE:
		return TemplatedCreatePrimitiveSegment<double>(linked_list);
	case PhysicalType::VARCHAR:
		// string_t
		// TODO: store string differently
		throw InternalException("LIST aggregate not yet implemented for strings!");
	default:
		// INT128, INTERVAL, STRUCT, LIST
		throw InternalException("LIST aggregate not yet implemented for " + TypeIdToString(type.InternalType()));
	}
}

ListSegment *CreateListSegment(LinkedList *linked_list) {

	uint16_t capacity = 4;
	if (linked_list->last_segment) {
		auto next_power_of_two = linked_list->last_segment->capacity * 2;
		capacity = next_power_of_two < 65536 ? next_power_of_two : linked_list->last_segment->capacity;
	}
	auto segment = (ListSegment *)AllocateListData(capacity);
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;

	auto linked_child_list = GetListChildData(segment);
	linked_child_list->first_segment = nullptr;
	linked_child_list->last_segment = nullptr;
	linked_child_list->total_capacity = 0;

	return segment;
}

void AppendRow(LinkedList *linked_list, const LogicalType &type, Vector &input, VectorData &input_data,
               idx_t &entry_idx) {

	ListSegment *segment = nullptr;

	// determine segment
	if (!linked_list->last_segment) {
		// no segments yet
		if (type.id() == LogicalTypeId::LIST) {
			segment = CreateListSegment(linked_list);
		} else if (type.id() == LogicalTypeId::STRUCT) {
			// TODO (focus on primitive types first)
		} else {
			segment = CreatePrimitiveSegment(linked_list, type);
		}
		linked_list->first_segment = segment;
		linked_list->last_segment = segment;

	} else if (linked_list->last_segment->capacity == linked_list->last_segment->count) {
		// last_segment is full
		if (type.id() == LogicalTypeId::LIST) {
			segment = CreateListSegment(linked_list);
		} else if (type.id() == LogicalTypeId::STRUCT) {
			// TODO (focus on primitive types first)
		} else {
			segment = CreatePrimitiveSegment(linked_list, type);
		}
		linked_list->last_segment->next = segment;
		linked_list->last_segment = segment;

	} else {
		// last_segment is not full, append to it
		segment = linked_list->last_segment;
	}

	// write null, the null_mask is always after the header info (capacity, count, next)
	auto null_mask = GetNullMask(segment);
	null_mask[segment->count] = !input_data.validity.RowIsValid(entry_idx);

	// write value
	if (type.id() == LogicalTypeId::LIST) {

		auto list_entries = (list_entry_t *)input_data.data;
		const auto &list_entry = list_entries[entry_idx];

		// get the child vector
		auto lists_size = ListVector::GetListSize(input);
		auto &child_vector = ListVector::GetEntry(input);
		VectorData child_data;
		child_vector.Orrify(lists_size, child_data);

		// write the list entry length and offset
		// by getting the length and offset of the previous list
		auto list_offset_data = GetListOffsetData(segment);
		list_offset_data[segment->count].length = list_entry.length;
		if (segment->count == 0) {
			list_offset_data[segment->count].offset = 0;
		} else {
			auto offset = list_offset_data[segment->count - 1].offset + list_offset_data[segment->count - 1].length;
			list_offset_data[segment->count].offset = offset;
		}

		// loop over the child entries and recurse on them
		// TODO: this can be done more efficiently maybe by writing them all at once
		// TODO: however, I think we maybe need the selection vector solution here, because the
		// TODO: child entries are not necessarily after each other, no?
		auto child_segments = GetListChildData(segment);
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
			auto source_idx = child_data.sel->get_index(list_entry.offset + child_idx);
			AppendRow(child_segments, child_vector.GetType(), child_vector, child_data, source_idx);
		}

	} else if (type.id() == LogicalTypeId::STRUCT) {
		// TODO (focus on primitive types first)

	} else {
		SetPrimitiveDataValue(segment, type, input_data.data, entry_idx);
	}

	linked_list->total_capacity++;
	segment->count++;
}

void BuildListVector(LinkedList *linked_list, Vector &aggr_vector) {

	auto &aggr_vector_validity = FlatVector::Validity(aggr_vector);
	idx_t total_count = 0;

	while (linked_list->first_segment) {
		auto segment = linked_list->first_segment;

		// set NULLs
		auto null_mask = GetNullMask(segment);
		for (idx_t i = 0; i < segment->count; i++) {
			if (null_mask[i]) {
				aggr_vector_validity.SetInvalid(total_count + i);
			}
		}

		if (aggr_vector.GetType().id() == LogicalTypeId::LIST) {
			auto list_vector_data = FlatVector::GetData<list_entry_t>(aggr_vector);

			// set offsets
			auto list_offset_data = GetListOffsetData(segment);
			for (idx_t i = 0; i < segment->count; i++) {
				if (aggr_vector_validity.RowIsValid(total_count + i)) {
					list_vector_data[total_count + i].length = list_offset_data[i].length;
					list_vector_data[total_count + i].offset = list_offset_data[i].offset;
				}
			}

			// TODO: resize child vector?
			auto &child_vector = ListVector::GetEntry(aggr_vector);
			auto linked_child_list = GetListChildData(segment);

			// recurse into the linked list of child values
			BuildListVector(linked_child_list, child_vector);

		} else if (aggr_vector.GetType().id() == LogicalTypeId::STRUCT) {
			// TODO: build struct vector

		} else {
			auto aggr_vector_data = FlatVector::GetData(aggr_vector);

			// set values
			for (idx_t i = 0; i < segment->count; i++) {
				if (aggr_vector_validity.RowIsValid(total_count + i)) {
					GetPrimitiveDataValue(segment, aggr_vector.GetType(), aggr_vector_data, i, total_count + i);
				}
			}
		}

		total_count += segment->count;
		linked_list->first_segment = segment->next;
		delete segment;
	}

	linked_list->last_segment = nullptr;
}

struct ListAggState {
	LinkedList *linked_list;
	LogicalType *type;
};

struct ListFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->linked_list = nullptr;
		state->type = nullptr;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->linked_list) {
			// TODO: do I need to recursively free/traverse all the segments?
			// TODO: or are they deleted during the finalize? Might be faster?
			delete state->linked_list;
		}
		if (state->type) {
			delete state->type;
		}
	}
	static bool IgnoreNull() {
		return false;
	}
};

static void ListUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector,
                               idx_t count) {
	D_ASSERT(input_count == 1);

	auto &input = inputs[0];
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);

	auto states = (ListAggState **)sdata.data;
	if (input.GetVectorType() == VectorType::SEQUENCE_VECTOR) {
		input.Flatten(count);
	}

	VectorData input_data;
	input.Orrify(count, input_data);

	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		if (!state->linked_list) {
			state->linked_list = new LinkedList;
			state->type = new LogicalType(input.GetType());
		}
		D_ASSERT(state->type);
		AppendRow(state->linked_list, *state->type, input, input_data, i);
	}
}

static void ListCombineFunction(Vector &state, Vector &combined, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state.ToUnifiedFormat(count, sdata);
	auto states_ptr = (ListAggState **)sdata.data;

	auto combined_ptr = FlatVector::GetData<ListAggState *>(combined);
	for (idx_t i = 0; i < count; i++) {
		auto state = states_ptr[sdata.sel->get_index(i)];
		if (!state->linked_list) {
			// NULL, no need to append.
			continue;
		}
		D_ASSERT(state->type);
		if (!combined_ptr[i]->linked_list) {
			combined_ptr[i]->linked_list = new LinkedList;
			combined_ptr[i]->linked_list->first_segment = state->linked_list->first_segment;
			combined_ptr[i]->linked_list->last_segment = state->linked_list->last_segment;
			combined_ptr[i]->linked_list->total_capacity = state->linked_list->total_capacity;
			combined_ptr[i]->type = new LogicalType(*state->type);
		} else {
			combined_ptr[i]->linked_list->last_segment->next = state->linked_list->first_segment;
			combined_ptr[i]->linked_list->last_segment = state->linked_list->last_segment;
			combined_ptr[i]->linked_list->total_capacity += state->linked_list->total_capacity;
		}
	}
}

static void ListFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (ListAggState **)sdata.data;

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	auto &mask = FlatVector::Validity(result);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	size_t total_len = ListVector::GetListSize(result);

	for (idx_t i = 0; i < count; i++) {

		auto state = states[sdata.sel->get_index(i)];
		const auto rid = i + offset;
		if (!state->linked_list) {
			mask.SetInvalid(rid);
			continue;
		}

		// set the length and offset of this list in the result vector
		auto total_capacity = state->linked_list->total_capacity;
		result_data[rid].length = total_capacity;
		result_data[rid].offset = total_len;
		total_len += total_capacity;

		D_ASSERT(state->type);

		Vector aggr_vector(*state->type, total_capacity);
		BuildListVector(state->linked_list, aggr_vector);
		ListVector::Append(result, aggr_vector, total_capacity);
	}
}

unique_ptr<FunctionData> ListBindFunction(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 1);
	function.return_type = LogicalType::LIST(arguments[0]->return_type);
	return nullptr;
}

void ListFun::RegisterFunction(BuiltinFunctions &set) {
	auto agg =
	    AggregateFunction("list", {LogicalType::ANY}, LogicalTypeId::LIST, AggregateFunction::StateSize<ListAggState>,
	                      AggregateFunction::StateInitialize<ListAggState, ListFunction>, ListUpdateFunction,
	                      ListCombineFunction, ListFinalize, nullptr, ListBindFunction,
	                      AggregateFunction::StateDestroy<ListAggState, ListFunction>, nullptr, nullptr);
	set.AddFunction(agg);
	agg.name = "array_agg";
	set.AddFunction(agg);
}

} // namespace duckdb
