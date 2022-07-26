#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

// allocate the header size, the size of the null_mask and the data size (capacity * T)
/*
    #3 -> {
        uint16_t count = 4;
        uint16_t capacity = 4;
        ListSegment *next = #4;
        bool null_mask[4] = { false, false, false, false };
        T values[4] = { 1, 2, 3, 4 }
        }
 */
template <class T>
data_ptr_t ListFun::AllocatePrimitiveData(Allocator &allocator, vector<unique_ptr<AllocatedData>> &owning_vector,
                                          uint16_t &capacity) {

	owning_vector.emplace_back(allocator.Allocate(sizeof(ListSegment) + capacity * (sizeof(bool) + sizeof(T))));
	return owning_vector.back()->get();
}

// allocate the header size, the size of the null_mask, the size of the list offsets (capacity * list_entry_t)
// and the linked list of the child entries
/*
    #1 -> {
        uint16_t count = 4;
        uint16_t capacity = 4;
        ListSegment *next = NULL;
        bool null_mask[4] = { false, false, true, false };
        list_entry_t offsets[4] = { {0, 3}, {3, 2}, _, {5, 4} };
        LinkedList children = { first = #3, last = #4 };
    }
 */
data_ptr_t ListFun::AllocateListData(Allocator &allocator, vector<unique_ptr<AllocatedData>> &owning_vector,
                                     uint16_t &capacity) {

	owning_vector.emplace_back(allocator.Allocate(
	    sizeof(ListSegment) + capacity * (sizeof(bool) + sizeof(list_entry_t)) + sizeof(LinkedList)));
	return owning_vector.back()->get();
}

// allocate the header size, the size of the null_mask and the size of the children pointers
/*
    #0 -> {
        uint16_t count = 4;
        uint16_t capacity = 4;
        ListSegment *next = NULL;
        bool null_mask[4] = { false, false, true, false };
        ListSegment *children[2] = { #1, #2 };
    };
 */
data_ptr_t ListFun::AllocateStructData(Allocator &allocator, vector<unique_ptr<AllocatedData>> &owning_vector,
                                       uint16_t &capacity, idx_t child_count) {

	owning_vector.emplace_back(
	    allocator.Allocate(sizeof(ListSegment) + capacity * sizeof(bool) + child_count * sizeof(ListSegment *)));
	return owning_vector.back()->get();
}

template <class T>
T *ListFun::TemplatedGetPrimitiveData(ListSegment *segment) {
	return (T *)(((char *)segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

void ListFun::GetPrimitiveDataValue(ListSegment *segment, const LogicalType &type, data_ptr_t &vector_data,
                                    idx_t &segment_idx, idx_t row_idx) {

	auto physical_type = type.InternalType();
	switch (physical_type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL: {
		auto data = TemplatedGetPrimitiveData<bool>(segment);
		((bool *)vector_data)[row_idx] = Load<bool>((data_ptr_t)(data + segment_idx));
		break;
	}
	case PhysicalType::INT8: {
		auto data = TemplatedGetPrimitiveData<int8_t>(segment);
		((int8_t *)vector_data)[row_idx] = Load<int8_t>((data_ptr_t)(data + segment_idx));
		break;
	}
	case PhysicalType::INT16: {
		auto data = TemplatedGetPrimitiveData<int16_t>(segment);
		((int16_t *)vector_data)[row_idx] = Load<int16_t>((data_ptr_t)(data + segment_idx));
		break;
	}
	case PhysicalType::INT32: {
		auto data = TemplatedGetPrimitiveData<int32_t>(segment);
		((int32_t *)vector_data)[row_idx] = Load<int32_t>((data_ptr_t)(data + segment_idx));
		break;
	}
	case PhysicalType::INT64: {
		auto data = TemplatedGetPrimitiveData<int64_t>(segment);
		((int64_t *)vector_data)[row_idx] = Load<int64_t>((data_ptr_t)(data + segment_idx));
		break;
	}
	case PhysicalType::UINT8: {
		auto data = TemplatedGetPrimitiveData<uint8_t>(segment);
		((uint8_t *)vector_data)[row_idx] = Load<uint8_t>((data_ptr_t)(data + segment_idx));
		break;
	}
	case PhysicalType::UINT16: {
		auto data = TemplatedGetPrimitiveData<uint16_t>(segment);
		((uint16_t *)vector_data)[row_idx] = Load<uint16_t>((data_ptr_t)(data + segment_idx));
		break;
	}
	case PhysicalType::UINT32: {
		auto data = TemplatedGetPrimitiveData<uint32_t>(segment);
		((uint32_t *)vector_data)[row_idx] = Load<uint32_t>((data_ptr_t)(data + segment_idx));
		break;
	}
	case PhysicalType::UINT64: {
		auto data = TemplatedGetPrimitiveData<uint64_t>(segment);
		((uint64_t *)vector_data)[row_idx] = Load<uint64_t>((data_ptr_t)(data + segment_idx));
		break;
	}
	case PhysicalType::FLOAT: {
		auto data = TemplatedGetPrimitiveData<float>(segment);
		((float *)vector_data)[row_idx] = Load<float>((data_ptr_t)(data + segment_idx));
		break;
	}
	case PhysicalType::DOUBLE: {
		auto data = TemplatedGetPrimitiveData<double>(segment);
		((double *)vector_data)[row_idx] = Load<double>((data_ptr_t)(data + segment_idx));
		break;
	}
	default:
		// INT128, INTERVAL
		throw InternalException("LIST aggregate not yet implemented for " + TypeIdToString(type.InternalType()));
	}
}

list_entry_t *ListFun::GetListOffsetData(ListSegment *segment) {
	return (list_entry_t *)(((char *)segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

LinkedList *ListFun::GetListChildData(ListSegment *segment) {
	return (LinkedList *)(((char *)segment) + sizeof(ListSegment) +
	                      segment->capacity * (sizeof(bool) + sizeof(list_entry_t)));
}

ListSegment **ListFun::GetStructData(ListSegment *segment) {
	return (ListSegment **)(((char *)segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

void ListFun::SetPrimitiveDataValue(ListSegment *segment, const LogicalType &type, data_ptr_t &input_data,
                                    idx_t &row_idx) {

	auto physical_type = type.InternalType();
	switch (physical_type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL: {
		auto data = TemplatedGetPrimitiveData<bool>(segment);
		Store<bool>(((bool *)input_data)[row_idx], (data_ptr_t)(data + segment->count));
		break;
	}
	case PhysicalType::INT8: {
		auto data = TemplatedGetPrimitiveData<int8_t>(segment);
		Store<int8_t>(((int8_t *)input_data)[row_idx], (data_ptr_t)(data + segment->count));
		break;
	}
	case PhysicalType::INT16: {
		auto data = TemplatedGetPrimitiveData<int16_t>(segment);
		Store<int16_t>(((int16_t *)input_data)[row_idx], (data_ptr_t)(data + segment->count));
		break;
	}
	case PhysicalType::INT32: {
		auto data = TemplatedGetPrimitiveData<int32_t>(segment);
		Store<int32_t>(((int32_t *)input_data)[row_idx], (data_ptr_t)(data + segment->count));
		break;
	}
	case PhysicalType::INT64: {
		auto data = TemplatedGetPrimitiveData<int64_t>(segment);
		Store<int64_t>(((int64_t *)input_data)[row_idx], (data_ptr_t)(data + segment->count));
		break;
	}
	case PhysicalType::UINT8: {
		auto data = TemplatedGetPrimitiveData<uint8_t>(segment);
		Store<uint8_t>(((uint8_t *)input_data)[row_idx], (data_ptr_t)(data + segment->count));
		break;
	}
	case PhysicalType::UINT16: {
		auto data = TemplatedGetPrimitiveData<uint16_t>(segment);
		Store<uint16_t>(((uint16_t *)input_data)[row_idx], (data_ptr_t)(data + segment->count));
		break;
	}
	case PhysicalType::UINT32: {
		auto data = TemplatedGetPrimitiveData<uint32_t>(segment);
		Store<uint32_t>(((uint32_t *)input_data)[row_idx], (data_ptr_t)(data + segment->count));
		break;
	}
	case PhysicalType::UINT64: {
		auto data = TemplatedGetPrimitiveData<uint64_t>(segment);
		Store<uint64_t>(((uint64_t *)input_data)[row_idx], (data_ptr_t)(data + segment->count));
		break;
	}
	case PhysicalType::FLOAT: {
		auto data = TemplatedGetPrimitiveData<float>(segment);
		Store<float>(((float *)input_data)[row_idx], (data_ptr_t)(data + segment->count));
		break;
	}
	case PhysicalType::DOUBLE: {
		auto data = TemplatedGetPrimitiveData<double>(segment);
		Store<double>(((double *)input_data)[row_idx], (data_ptr_t)(data + segment->count));
		break;
	}
	default:
		// INT128, INTERVAL, STRUCT, LIST
		throw InternalException("LIST aggregate not yet implemented for " + TypeIdToString(type.InternalType()));
	}
}

// this is the same for all segment types
bool *ListFun::GetNullMask(ListSegment *segment) {
	return (bool *)(((char *)segment) + sizeof(ListSegment));
}

uint16_t ListFun::GetCapacityForNewSegment(LinkedList *linked_list) {

	uint16_t capacity = 4;
	if (linked_list->last_segment) {
		auto next_power_of_two = linked_list->last_segment->capacity * 2;
		capacity = next_power_of_two < 65536 ? next_power_of_two : linked_list->last_segment->capacity;
	}
	return capacity;
}

template <class T>
ListSegment *ListFun::TemplatedCreatePrimitiveSegment(Allocator &allocator,
                                                      vector<unique_ptr<AllocatedData>> &owning_vector,
                                                      uint16_t &capacity) {

	auto segment = (ListSegment *)AllocatePrimitiveData<T>(allocator, owning_vector, capacity);
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;
	return segment;
}

ListSegment *ListFun::CreatePrimitiveSegment(Allocator &allocator, vector<unique_ptr<AllocatedData>> &owning_vector,
                                             uint16_t &capacity, const LogicalType &type) {

	auto physical_type = type.InternalType();

	switch (physical_type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL:
		return TemplatedCreatePrimitiveSegment<bool>(allocator, owning_vector, capacity);
	case PhysicalType::INT8:
		return TemplatedCreatePrimitiveSegment<int8_t>(allocator, owning_vector, capacity);
	case PhysicalType::INT16:
		return TemplatedCreatePrimitiveSegment<int16_t>(allocator, owning_vector, capacity);
	case PhysicalType::INT32:
		return TemplatedCreatePrimitiveSegment<int32_t>(allocator, owning_vector, capacity);
	case PhysicalType::INT64:
		return TemplatedCreatePrimitiveSegment<int64_t>(allocator, owning_vector, capacity);
	case PhysicalType::UINT8:
		return TemplatedCreatePrimitiveSegment<uint8_t>(allocator, owning_vector, capacity);
	case PhysicalType::UINT16:
		return TemplatedCreatePrimitiveSegment<uint16_t>(allocator, owning_vector, capacity);
	case PhysicalType::UINT32:
		return TemplatedCreatePrimitiveSegment<uint32_t>(allocator, owning_vector, capacity);
	case PhysicalType::UINT64:
		return TemplatedCreatePrimitiveSegment<uint64_t>(allocator, owning_vector, capacity);
	case PhysicalType::FLOAT:
		return TemplatedCreatePrimitiveSegment<float>(allocator, owning_vector, capacity);
	case PhysicalType::DOUBLE:
		return TemplatedCreatePrimitiveSegment<double>(allocator, owning_vector, capacity);
	case PhysicalType::VARCHAR:
		return TemplatedCreatePrimitiveSegment<char>(allocator, owning_vector, capacity);
	default:
		// INT128, INTERVAL
		throw InternalException("LIST aggregate not yet implemented for " + TypeIdToString(type.InternalType()));
	}
}

ListSegment *ListFun::CreateListSegment(Allocator &allocator, vector<unique_ptr<AllocatedData>> &owning_vector,
                                        uint16_t &capacity) {

	auto segment = (ListSegment *)AllocateListData(allocator, owning_vector, capacity);
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;

	auto linked_child_list = GetListChildData(segment);
	LinkedList linked_list(0, nullptr, nullptr);
	Store<LinkedList>(linked_list, (data_ptr_t)linked_child_list);

	return segment;
}

ListSegment *ListFun::CreateStructSegment(Allocator &allocator, vector<unique_ptr<AllocatedData>> &owning_vector,
                                          uint16_t &capacity, vector<unique_ptr<Vector>> &children) {

	auto segment = (ListSegment *)AllocateStructData(allocator, owning_vector, capacity, children.size());
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;

	// now create a child segment with exactly the same capacity for each child
	auto child_segments = GetStructData(segment);
	for (idx_t i = 0; i < children.size(); i++) {
		auto child_segment = CreateSegment(allocator, owning_vector, capacity, *children[i]);
		Store<ListSegment *>(child_segment, (data_ptr_t)(child_segments + i));
	}

	return segment;
}

ListSegment *ListFun::CreateSegment(Allocator &allocator, vector<unique_ptr<AllocatedData>> &owning_vector,
                                    uint16_t &capacity, Vector &input) {

	if (input.GetType().InternalType() == PhysicalType::VARCHAR) {
		return CreateListSegment(allocator, owning_vector, capacity);
	} else if (input.GetType().id() == LogicalTypeId::LIST) {
		return CreateListSegment(allocator, owning_vector, capacity);
	} else if (input.GetType().id() == LogicalTypeId::STRUCT) {
		auto &children = StructVector::GetEntries(input);
		return CreateStructSegment(allocator, owning_vector, capacity, children);
	}
	return CreatePrimitiveSegment(allocator, owning_vector, capacity, input.GetType());
}

ListSegment *ListFun::GetSegment(Allocator &allocator, vector<unique_ptr<AllocatedData>> &owning_vector,
                                 LinkedList *linked_list, Vector &input) {

	ListSegment *segment = nullptr;

	// determine segment
	if (!linked_list->last_segment) {
		// no segments yet
		auto capacity = GetCapacityForNewSegment(linked_list);
		segment = CreateSegment(allocator, owning_vector, capacity, input);
		linked_list->first_segment = segment;
		linked_list->last_segment = segment;

	} else if (linked_list->last_segment->capacity == linked_list->last_segment->count) {
		// last_segment is full
		auto capacity = GetCapacityForNewSegment(linked_list);
		segment = CreateSegment(allocator, owning_vector, capacity, input);
		linked_list->last_segment->next = segment;
		linked_list->last_segment = segment;

	} else {
		// last_segment is not full, append to it
		segment = linked_list->last_segment;
	}

	D_ASSERT(segment);
	return segment;
}

ListSegment *ListFun::GetCharSegment(Allocator &allocator, vector<unique_ptr<AllocatedData>> &owning_vector,
                                     LinkedList *linked_list) {

	ListSegment *segment = nullptr;

	// determine segment
	if (!linked_list->last_segment) {
		// no segments yet
		auto capacity = GetCapacityForNewSegment(linked_list);
		segment = CreatePrimitiveSegment(allocator, owning_vector, capacity, LogicalTypeId::VARCHAR);
		linked_list->first_segment = segment;
		linked_list->last_segment = segment;

	} else if (linked_list->last_segment->capacity == linked_list->last_segment->count) {
		// last_segment is full
		auto capacity = GetCapacityForNewSegment(linked_list);
		segment = CreatePrimitiveSegment(allocator, owning_vector, capacity, LogicalTypeId::VARCHAR);
		linked_list->last_segment->next = segment;
		linked_list->last_segment = segment;

	} else {
		// last_segment is not full, append to it
		segment = linked_list->last_segment;
	}

	D_ASSERT(segment);
	return segment;
}

void ListFun::WriteDataToSegment(Allocator &allocator, vector<unique_ptr<AllocatedData>> &owning_vector,
                                 ListSegment *segment, Vector &input, idx_t &entry_idx, idx_t &count) {

	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(count, input_data);

	// write null, the null_mask is always after the header info (capacity, count, next)
	auto null_mask = GetNullMask(segment);
	null_mask[segment->count] = !input_data.validity.RowIsValid(entry_idx);

	// write value
	if (input.GetType().InternalType() == PhysicalType::VARCHAR) {

		// get the string
		auto str_t = ((string_t *)input_data.data)[entry_idx];
		auto str = str_t.GetString();

		// set the length and offset of this string
		auto list_offset_data = GetListOffsetData(segment);
		list_entry_t list_entry;
		list_entry.length = str_t.GetSize();
		if (segment->count == 0) {
			list_entry.offset = 0;
		} else {
			auto prev_list_entry = Load<list_entry_t>((data_ptr_t)(list_offset_data + segment->count - 1));
			list_entry.offset = prev_list_entry.offset + prev_list_entry.length;
		}
		Store<list_entry_t>(list_entry, (data_ptr_t)(list_offset_data + segment->count));

		// write the characters to the child segments
		auto child_segments = Load<LinkedList>((data_ptr_t)GetListChildData(segment));
		for (char &c : str) {
			auto child_segment = GetCharSegment(allocator, owning_vector, &child_segments);
			auto data = TemplatedGetPrimitiveData<char>(child_segment);
			data[child_segment->count] = c;
			child_segment->count++;
			child_segments.total_capacity++;
		}

		// store the updated linked list
		Store<LinkedList>(child_segments, (data_ptr_t)GetListChildData(segment));

	} else if (input.GetType().id() == LogicalTypeId::LIST) {

		auto list_entries = (list_entry_t *)input_data.data;
		const auto &list_entry = list_entries[entry_idx];

		// get the child vector
		auto lists_size = ListVector::GetListSize(input);
		auto &child_vector = ListVector::GetEntry(input);
		UnifiedVectorFormat child_data;
		child_vector.ToUnifiedFormat(lists_size, child_data);

		// write the list entry length and offset
		// by getting the length and offset of the previous list
		auto list_offset_data = GetListOffsetData(segment);
		list_entry_t list_offset_entry;
		list_offset_entry.length = list_entry.length;
		if (segment->count == 0) {
			list_offset_entry.offset = 0;
		} else {
			auto prev_list_entry = Load<list_entry_t>((data_ptr_t)(list_offset_data + segment->count - 1));
			list_offset_entry.offset = prev_list_entry.offset + prev_list_entry.length;
		}
		Store<list_entry_t>(list_offset_entry, (data_ptr_t)(list_offset_data + segment->count));

		// loop over the child entries and recurse on them
		auto child_segments = Load<LinkedList>((data_ptr_t)GetListChildData(segment));
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
			auto source_idx = child_data.sel->get_index(list_entry.offset + child_idx);
			AppendRow(allocator, owning_vector, &child_segments, child_vector, source_idx, lists_size);
		}
		// store the updated linked list
		Store<LinkedList>(child_segments, (data_ptr_t)GetListChildData(segment));

	} else if (input.GetType().id() == LogicalTypeId::STRUCT) {

		auto &children = StructVector::GetEntries(input);
		auto child_list = GetStructData(segment);

		// write the data of each of the children of the struct
		for (idx_t i = 0; i < children.size(); i++) {
			auto child_list_segment = Load<ListSegment *>((data_ptr_t)(child_list + i));
			WriteDataToSegment(allocator, owning_vector, child_list_segment, *children[i], entry_idx, count);
			child_list_segment->count++;
		}

	} else {
		SetPrimitiveDataValue(segment, input.GetType(), input_data.data, entry_idx);
	}
}

void ListFun::AppendRow(Allocator &allocator, vector<unique_ptr<AllocatedData>> &owning_vector, LinkedList *linked_list,
                        Vector &input, idx_t &entry_idx, idx_t &count) {

	auto segment = GetSegment(allocator, owning_vector, linked_list, input);
	WriteDataToSegment(allocator, owning_vector, segment, input, entry_idx, count);

	linked_list->total_capacity++;
	segment->count++;
}

void ListFun::GetDataFromSegment(ListSegment *segment, Vector &result, idx_t &total_count) {

	auto &aggr_vector_validity = FlatVector::Validity(result);

	// set NULLs
	auto null_mask = GetNullMask(segment);
	for (idx_t i = 0; i < segment->count; i++) {
		if (null_mask[i]) {
			aggr_vector_validity.SetInvalid(total_count + i);
		}
	}

	if (result.GetType().InternalType() == PhysicalType::VARCHAR) {
		// append all the child chars to one string
		string str = "";
		auto linked_child_list = Load<LinkedList>((data_ptr_t)GetListChildData(segment));
		while (linked_child_list.first_segment) {
			auto child_segment = linked_child_list.first_segment;
			auto data = TemplatedGetPrimitiveData<char>(child_segment);
			str.append(data, child_segment->count);
			linked_child_list.first_segment = child_segment->next;
		}
		linked_child_list.last_segment = nullptr;

		// use length and offset to set values
		auto aggr_vector_data = FlatVector::GetData(result);
		auto list_offset_data = GetListOffsetData(segment);

		for (idx_t i = 0; i < segment->count; i++) {
			auto list_entry = Load<list_entry_t>((data_ptr_t)(list_offset_data + i));
			auto substr = str.substr(list_entry.offset, list_entry.length);
			auto str_t = StringVector::AddString(result, substr);
			((string_t *)aggr_vector_data)[total_count + i] = str_t;
		}

	} else if (result.GetType().id() == LogicalTypeId::LIST) {
		auto list_vector_data = FlatVector::GetData<list_entry_t>(result);

		// set offsets
		auto list_offset_data = GetListOffsetData(segment);
		for (idx_t i = 0; i < segment->count; i++) {
			if (aggr_vector_validity.RowIsValid(total_count + i)) {
				auto list_offset_entry = Load<list_entry_t>((data_ptr_t)(list_offset_data + i));
				list_vector_data[total_count + i].length = list_offset_entry.length;
				list_vector_data[total_count + i].offset = list_offset_entry.offset;
			}
		}

		auto &child_vector = ListVector::GetEntry(result);
		auto linked_child_list = Load<LinkedList>((data_ptr_t)GetListChildData(segment));
		ListVector::Reserve(result, linked_child_list.total_capacity);

		// recurse into the linked list of child values
		BuildListVector(&linked_child_list, child_vector);

	} else if (result.GetType().id() == LogicalTypeId::STRUCT) {
		auto &children = StructVector::GetEntries(result);

		auto struct_children = GetStructData(segment);
		for (idx_t child_count = 0; child_count < children.size(); child_count++) {
			auto struct_children_segment = Load<ListSegment *>((data_ptr_t)(struct_children + child_count));
			GetDataFromSegment(struct_children_segment, *children[child_count], total_count);
		}

	} else {
		auto aggr_vector_data = FlatVector::GetData(result);

		// set values
		for (idx_t i = 0; i < segment->count; i++) {
			if (aggr_vector_validity.RowIsValid(total_count + i)) {
				GetPrimitiveDataValue(segment, result.GetType(), aggr_vector_data, i, total_count + i);
			}
		}
	}
}

void ListFun::BuildListVector(LinkedList *linked_list, Vector &result) {

	idx_t total_count = 0;
	while (linked_list->first_segment) {
		auto segment = linked_list->first_segment;
		GetDataFromSegment(segment, result, total_count);

		total_count += segment->count;
		linked_list->first_segment = segment->next;
	}

	linked_list->last_segment = nullptr;
}

struct ListAggState {
	LinkedList *linked_list;
	LogicalType *type;
	vector<unique_ptr<AllocatedData>> *owning_vector;
};

struct ListFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->linked_list = nullptr;
		state->type = nullptr;
		state->owning_vector = nullptr;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		D_ASSERT(state);
		if (state->linked_list) {
			delete state->linked_list;
		}
		if (state->type) {
			delete state->type;
		}
		if (state->owning_vector) {
			state->owning_vector->clear();
			delete state->owning_vector;
		}
	}
	static bool IgnoreNull() {
		return false;
	}
};

static void ListUpdateFunction(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                               Vector &state_vector, idx_t count) {
	D_ASSERT(input_count == 1);

	auto &input = inputs[0];
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);

	auto states = (ListAggState **)sdata.data;
	if (input.GetVectorType() == VectorType::SEQUENCE_VECTOR) {
		input.Flatten(count);
	}

	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		if (!state->linked_list) {
			state->linked_list = new LinkedList(0, nullptr, nullptr);
			state->type = new LogicalType(input.GetType());
			state->owning_vector = new vector<unique_ptr<AllocatedData>>;
		}
		D_ASSERT(state->type);
		ListFun::AppendRow(aggr_input_data.allocator, *state->owning_vector, state->linked_list, input, i, count);
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
		D_ASSERT(state->owning_vector);
		if (!combined_ptr[i]->linked_list) {

			// copy the linked list
			combined_ptr[i]->linked_list = new LinkedList(0, nullptr, nullptr);
			combined_ptr[i]->linked_list->first_segment = state->linked_list->first_segment;
			combined_ptr[i]->linked_list->last_segment = state->linked_list->last_segment;
			combined_ptr[i]->linked_list->total_capacity = state->linked_list->total_capacity;

			// copy the type
			combined_ptr[i]->type = new LogicalType(*state->type);

			// new owning_vector to hold the unique pointers
			combined_ptr[i]->owning_vector = new vector<unique_ptr<AllocatedData>>;

		} else {
			combined_ptr[i]->linked_list->last_segment->next = state->linked_list->first_segment;
			combined_ptr[i]->linked_list->last_segment = state->linked_list->last_segment;
			combined_ptr[i]->linked_list->total_capacity += state->linked_list->total_capacity;
		}

		// copy the owning vector (and its unique pointers to the allocated data)
		// FIXME: more efficient way of copying the unique pointers?
		auto &owning_vector = *state->owning_vector;
		for (idx_t j = 0; j < state->owning_vector->size(); j++) {
			combined_ptr[i]->owning_vector->push_back(move(owning_vector[j]));
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
		ListFun::BuildListVector(state->linked_list, aggr_vector);
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
