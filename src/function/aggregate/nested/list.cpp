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
	uint16_t total_capacity = 0;
	ListSegment *first_segment = nullptr;
	ListSegment *last_segment = nullptr;
};

// allocate the header size, the size of the null_mask and the data size (capacity * T)
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

void GetPrimitiveDataValue(ListSegment *segment, const LogicalType &type, data_ptr_t &vector_data,
                           uint16_t &segment_idx, idx_t &row_idx) {

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
		capacity = linked_list->last_segment->capacity * 2;
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

void AppendRow(LinkedList *linked_list, const LogicalType &type, VectorData &input_data, idx_t &row_idx) {

	ListSegment *segment = nullptr;

	// determine segment
	if (!linked_list->last_segment) {
		// no segments yet
		if (type.id() == LogicalTypeId::LIST) {
			// TODO (focus on primitive types first)
		} else if (type.id() == LogicalTypeId::STRUCT) {
			// TODO (focus on primitive types first)
		} else {
			// create a segment for primitive types
			segment = CreatePrimitiveSegment(linked_list, type);
			linked_list->first_segment = segment;
			linked_list->last_segment = segment;
		}
	} else if (linked_list->last_segment->capacity == linked_list->last_segment->count) {
		// last_segment is full
		segment = CreatePrimitiveSegment(linked_list, type);
		linked_list->last_segment->next = segment;
		linked_list->last_segment = segment;
	} else {
		// last_segment is not full
		segment = linked_list->last_segment;
	}

	// write null
	auto null_mask = GetNullMask(segment);
	null_mask[segment->count] = !input_data.validity.RowIsValid(row_idx);

	// write value
	if (type.id() == LogicalTypeId::LIST) {
		// TODO (focus on primitive types first)
	} else if (type.id() == LogicalTypeId::STRUCT) {
		// TODO (focus on primitive types first)
	} else {
		SetPrimitiveDataValue(segment, type, input_data.data, row_idx);
	}

	linked_list->total_capacity++;
	segment->count++;
}

void BuildListVector(LinkedList *linked_list, Vector &aggr_vector) {

	// TODO: do this for non-primitive types

	auto aggr_vector_data = FlatVector::GetData(aggr_vector);
	auto &aggr_vector_validity = FlatVector::Validity(aggr_vector);

	idx_t total_count = 0;
	uint16_t idx_in_segment = 0;
	// append all values to the result vector and delete the segments
	while (total_count < (idx_t)linked_list->total_capacity) {

		auto segment = linked_list->first_segment;

		auto null_mask = GetNullMask(segment);
		if (null_mask[idx_in_segment]) {
			// element is null, set invalid
			aggr_vector_validity.SetInvalid(total_count);
		} else {
			// not null, get the data value
			GetPrimitiveDataValue(segment, aggr_vector.GetType(), aggr_vector_data, idx_in_segment, total_count);
		}

		idx_in_segment++;
		total_count++;
		if (idx_in_segment == segment->count) {
			linked_list->first_segment = segment->next;
			delete segment;
			idx_in_segment = 0;
		}
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
	VectorData sdata;
	state_vector.Orrify(count, sdata);

	auto states = (ListAggState **)sdata.data;
	if (input.GetVectorType() == VectorType::SEQUENCE_VECTOR) {
		input.Normalify(count);
	}

	VectorData input_data;
	input.Orrify(count, input_data);

	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		if (!state->linked_list) {
			state->linked_list = new LinkedList;
			state->type = new LogicalType(input.GetType());
		}
		AppendRow(state->linked_list, input.GetType(), input_data, i);
	}
}

static void ListCombineFunction(Vector &state, Vector &combined, AggregateInputData &, idx_t count) {
	VectorData sdata;
	state.Orrify(count, sdata);
	auto states_ptr = (ListAggState **)sdata.data;

	auto combined_ptr = FlatVector::GetData<ListAggState *>(combined);
	for (idx_t i = 0; i < count; i++) {
		auto state = states_ptr[sdata.sel->get_index(i)];
		if (!state->linked_list) {
			// NULL, no need to append.
			continue;
		}
		if (!combined_ptr[i]->linked_list) {
			combined_ptr[i]->linked_list = new LinkedList;
			combined_ptr[i]->linked_list->first_segment = state->linked_list->first_segment;
			combined_ptr[i]->linked_list->last_segment = state->linked_list->last_segment;
			combined_ptr[i]->linked_list->total_capacity = state->linked_list->total_capacity;
		} else {
			combined_ptr[i]->linked_list->last_segment->next = state->linked_list->first_segment;
			combined_ptr[i]->linked_list->last_segment = state->linked_list->last_segment;
			combined_ptr[i]->linked_list->total_capacity += state->linked_list->total_capacity;
		}
	}
}

static void ListFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	VectorData sdata;
	state_vector.Orrify(count, sdata);
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

		// TODO: also allow total_capacity > 1024
		// TODO: is this the correct type?

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
