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
	LinkedList() {};
	LinkedList(idx_t total_capacity_p, ListSegment *first_segment_p, ListSegment *last_segment_p)
	    : total_capacity(total_capacity_p), first_segment(first_segment_p), last_segment(last_segment_p) {
	}

	idx_t total_capacity = 0;
	ListSegment *first_segment = nullptr;
	ListSegment *last_segment = nullptr;
};

// forward declarations
struct WriteDataToSegment;
struct ReadDataFromSegment;
typedef ListSegment *(*create_segment_t)(WriteDataToSegment &write_data_to_segment, Allocator &allocator,
                                         vector<AllocatedData> &owning_vector, uint16_t &capacity);
typedef void (*write_data_to_segment_t)(WriteDataToSegment &write_data_to_segment, Allocator &allocator,
                                        vector<AllocatedData> &owning_vector, ListSegment *segment, Vector &input,
                                        idx_t &entry_idx, idx_t &count);
typedef void (*read_data_from_segment_t)(ReadDataFromSegment &read_data_from_segment, ListSegment *segment,
                                         Vector &result, idx_t &total_count);

struct WriteDataToSegment {
	create_segment_t create_segment;
	write_data_to_segment_t segment_function;
	vector<WriteDataToSegment> child_functions;
};
struct ReadDataFromSegment {
	read_data_from_segment_t segment_function;
	vector<ReadDataFromSegment> child_functions;
};

// forward declarations
static void AppendRow(WriteDataToSegment &write_data_to_segment, Allocator &allocator,
                      vector<AllocatedData> &owning_vector, LinkedList *linked_list, Vector &input, idx_t &entry_idx,
                      idx_t &count);
static void BuildListVector(ReadDataFromSegment &read_data_from_segment, LinkedList *linked_list, Vector &result,
                            idx_t &initial_total_count);

template <class T>
static data_ptr_t AllocatePrimitiveData(Allocator &allocator, vector<AllocatedData> &owning_vector,
                                        uint16_t &capacity) {

	owning_vector.emplace_back(allocator.Allocate(sizeof(ListSegment) + capacity * (sizeof(bool) + sizeof(T))));
	return owning_vector.back().get();
}

static data_ptr_t AllocateListData(Allocator &allocator, vector<AllocatedData> &owning_vector, uint16_t &capacity) {

	owning_vector.emplace_back(
	    allocator.Allocate(sizeof(ListSegment) + capacity * (sizeof(bool) + sizeof(uint64_t)) + sizeof(LinkedList)));
	return owning_vector.back().get();
}

static data_ptr_t AllocateStructData(Allocator &allocator, vector<AllocatedData> &owning_vector, uint16_t &capacity,
                                     idx_t child_count) {

	owning_vector.emplace_back(
	    allocator.Allocate(sizeof(ListSegment) + capacity * sizeof(bool) + child_count * sizeof(ListSegment *)));
	return owning_vector.back().get();
}

template <class T>
static T *GetPrimitiveData(ListSegment *segment) {
	return (T *)(((char *)segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

static uint64_t *GetListLengthData(ListSegment *segment) {
	return (uint64_t *)(((char *)segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

static LinkedList *GetListChildData(ListSegment *segment) {
	return (LinkedList *)(((char *)segment) + sizeof(ListSegment) +
	                      segment->capacity * (sizeof(bool) + sizeof(uint64_t)));
}

static ListSegment **GetStructData(ListSegment *segment) {
	return (ListSegment **)(((char *)segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

static bool *GetNullMask(ListSegment *segment) {
	return (bool *)(((char *)segment) + sizeof(ListSegment));
}

static uint16_t GetCapacityForNewSegment(LinkedList *linked_list) {

	// consecutive segments grow by the power of two
	uint16_t capacity = 4;
	if (linked_list->last_segment) {
		auto next_power_of_two = linked_list->last_segment->capacity * 2;
		capacity = next_power_of_two < 65536 ? next_power_of_two : linked_list->last_segment->capacity;
	}
	return capacity;
}

template <class T>
static ListSegment *CreatePrimitiveSegment(WriteDataToSegment &, Allocator &allocator,
                                           vector<AllocatedData> &owning_vector, uint16_t &capacity) {

	// allocate data and set the header
	auto segment = (ListSegment *)AllocatePrimitiveData<T>(allocator, owning_vector, capacity);
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;
	return segment;
}

static ListSegment *CreateListSegment(WriteDataToSegment &, Allocator &allocator, vector<AllocatedData> &owning_vector,
                                      uint16_t &capacity) {

	// allocate data and set the header
	auto segment = (ListSegment *)AllocateListData(allocator, owning_vector, capacity);
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;

	// create an empty linked list for the child vector
	auto linked_child_list = GetListChildData(segment);
	LinkedList linked_list(0, nullptr, nullptr);
	Store<LinkedList>(linked_list, (data_ptr_t)linked_child_list);

	return segment;
}

static ListSegment *CreateStructSegment(WriteDataToSegment &write_data_to_segment, Allocator &allocator,
                                        vector<AllocatedData> &owning_vector, uint16_t &capacity) {

	// allocate data and set header
	auto segment = (ListSegment *)AllocateStructData(allocator, owning_vector, capacity,
	                                                 write_data_to_segment.child_functions.size());
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;

	// create a child ListSegment with exactly the same capacity for each child vector
	auto child_segments = GetStructData(segment);
	for (idx_t i = 0; i < write_data_to_segment.child_functions.size(); i++) {
		auto child_function = write_data_to_segment.child_functions[i];
		auto child_segment = child_function.create_segment(child_function, allocator, owning_vector, capacity);
		Store<ListSegment *>(child_segment, (data_ptr_t)(child_segments + i));
	}

	return segment;
}

static ListSegment *GetSegment(WriteDataToSegment &write_data_to_segment, Allocator &allocator,
                               vector<AllocatedData> &owning_vector, LinkedList *linked_list) {

	ListSegment *segment = nullptr;

	// determine segment
	if (!linked_list->last_segment) {
		// empty linked list, create the first (and last) segment
		auto capacity = GetCapacityForNewSegment(linked_list);
		segment = write_data_to_segment.create_segment(write_data_to_segment, allocator, owning_vector, capacity);
		linked_list->first_segment = segment;
		linked_list->last_segment = segment;

	} else if (linked_list->last_segment->capacity == linked_list->last_segment->count) {
		// the last segment of the linked list is full, create a new one and append it
		auto capacity = GetCapacityForNewSegment(linked_list);
		segment = write_data_to_segment.create_segment(write_data_to_segment, allocator, owning_vector, capacity);
		linked_list->last_segment->next = segment;
		linked_list->last_segment = segment;

	} else {
		// the last segment of the linked list is not full, append the data to it
		segment = linked_list->last_segment;
	}

	D_ASSERT(segment);
	return segment;
}

template <class T>
static void WriteDataToPrimitiveSegment(WriteDataToSegment &, Allocator &allocator,
                                        vector<AllocatedData> &owning_vector, ListSegment *segment, Vector &input,
                                        idx_t &entry_idx, idx_t &count) {

	// get the vector data and the source index of the entry that we want to write
	auto input_data = FlatVector::GetData(input);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto is_null = FlatVector::IsNull(input, entry_idx);
	null_mask[segment->count] = is_null;

	// write value
	if (!is_null) {
		auto data = GetPrimitiveData<T>(segment);
		Store<T>(((T *)input_data)[entry_idx], (data_ptr_t)(data + segment->count));
	}
}

static void WriteDataToVarcharSegment(WriteDataToSegment &write_data_to_segment, Allocator &allocator,
                                      vector<AllocatedData> &owning_vector, ListSegment *segment, Vector &input,
                                      idx_t &entry_idx, idx_t &count) {

	// get the vector data and the source index of the entry that we want to write
	auto input_data = FlatVector::GetData(input);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto is_null = FlatVector::IsNull(input, entry_idx);
	null_mask[segment->count] = is_null;

	// set the length of this string
	auto str_length_data = GetListLengthData(segment);
	uint64_t str_length = 0;

	// get the string
	string_t str_t;
	if (!is_null) {
		str_t = ((string_t *)input_data)[entry_idx];
		str_length = str_t.GetSize();
	}

	// we can reconstruct the offset from the length
	Store<uint64_t>(str_length, (data_ptr_t)(str_length_data + segment->count));

	if (is_null) {
		return;
	}

	// write the characters to the linked list of child segments
	auto child_segments = Load<LinkedList>((data_ptr_t)GetListChildData(segment));
	for (char &c : str_t.GetString()) {
		auto child_segment =
		    GetSegment(write_data_to_segment.child_functions.back(), allocator, owning_vector, &child_segments);
		auto data = GetPrimitiveData<char>(child_segment);
		data[child_segment->count] = c;
		child_segment->count++;
		child_segments.total_capacity++;
	}

	// store the updated linked list
	Store<LinkedList>(child_segments, (data_ptr_t)GetListChildData(segment));
}

static void WriteDataToListSegment(WriteDataToSegment &write_data_to_segment, Allocator &allocator,
                                   vector<AllocatedData> &owning_vector, ListSegment *segment, Vector &input,
                                   idx_t &entry_idx, idx_t &count) {

	// get the vector data and the source index of the entry that we want to write
	auto input_data = FlatVector::GetData(input);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto is_null = FlatVector::IsNull(input, entry_idx);
	null_mask[segment->count] = is_null;

	// set the length of this list
	auto list_length_data = GetListLengthData(segment);
	uint64_t list_length = 0;

	if (!is_null) {
		// get list entry information
		auto list_entries = (list_entry_t *)input_data;
		const auto &list_entry = list_entries[entry_idx];
		list_length = list_entry.length;

		// get the child vector and its data
		auto lists_size = ListVector::GetListSize(input);
		auto &child_vector = ListVector::GetEntry(input);

		// loop over the child vector entries and recurse on them
		auto child_segments = Load<LinkedList>((data_ptr_t)GetListChildData(segment));
		D_ASSERT(write_data_to_segment.child_functions.size() == 1);
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
			auto source_idx_child = list_entry.offset + child_idx;
			AppendRow(write_data_to_segment.child_functions[0], allocator, owning_vector, &child_segments, child_vector,
			          source_idx_child, lists_size);
		}
		// store the updated linked list
		Store<LinkedList>(child_segments, (data_ptr_t)GetListChildData(segment));
	}

	Store<uint64_t>(list_length, (data_ptr_t)(list_length_data + segment->count));
}

static void WriteDataToStructSegment(WriteDataToSegment &write_data_to_segment, Allocator &allocator,
                                     vector<AllocatedData> &owning_vector, ListSegment *segment, Vector &input,
                                     idx_t &entry_idx, idx_t &count) {

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto is_null = FlatVector::IsNull(input, entry_idx);
	null_mask[segment->count] = is_null;

	// write value
	auto &children = StructVector::GetEntries(input);
	D_ASSERT(children.size() == write_data_to_segment.child_functions.size());
	auto child_list = GetStructData(segment);

	// write the data of each of the children of the struct
	for (idx_t child_count = 0; child_count < children.size(); child_count++) {
		auto child_list_segment = Load<ListSegment *>((data_ptr_t)(child_list + child_count));
		auto &child_function = write_data_to_segment.child_functions[child_count];
		child_function.segment_function(child_function, allocator, owning_vector, child_list_segment,
		                                *children[child_count], entry_idx, count);
		child_list_segment->count++;
	}
}

static void AppendRow(WriteDataToSegment &write_data_to_segment, Allocator &allocator,
                      vector<AllocatedData> &owning_vector, LinkedList *linked_list, Vector &input, idx_t &entry_idx,
                      idx_t &count) {

	D_ASSERT(input.GetVectorType() == VectorType::FLAT_VECTOR);

	auto segment = GetSegment(write_data_to_segment, allocator, owning_vector, linked_list);
	write_data_to_segment.segment_function(write_data_to_segment, allocator, owning_vector, segment, input, entry_idx,
	                                       count);

	linked_list->total_capacity++;
	segment->count++;
}

template <class T>
static void ReadDataFromPrimitiveSegment(ReadDataFromSegment &, ListSegment *segment, Vector &result,
                                         idx_t &total_count) {

	auto &aggr_vector_validity = FlatVector::Validity(result);

	// set NULLs
	auto null_mask = GetNullMask(segment);
	for (idx_t i = 0; i < segment->count; i++) {
		if (null_mask[i]) {
			aggr_vector_validity.SetInvalid(total_count + i);
		}
	}

	auto aggr_vector_data = FlatVector::GetData(result);

	// load values
	for (idx_t i = 0; i < segment->count; i++) {
		if (aggr_vector_validity.RowIsValid(total_count + i)) {
			auto data = GetPrimitiveData<T>(segment);
			((T *)aggr_vector_data)[total_count + i] = Load<T>((data_ptr_t)(data + i));
		}
	}
}

static void ReadDataFromVarcharSegment(ReadDataFromSegment &, ListSegment *segment, Vector &result,
                                       idx_t &total_count) {

	auto &aggr_vector_validity = FlatVector::Validity(result);

	// set NULLs
	auto null_mask = GetNullMask(segment);
	for (idx_t i = 0; i < segment->count; i++) {
		if (null_mask[i]) {
			aggr_vector_validity.SetInvalid(total_count + i);
		}
	}

	// append all the child chars to one string
	string str = "";
	auto linked_child_list = Load<LinkedList>((data_ptr_t)GetListChildData(segment));
	while (linked_child_list.first_segment) {
		auto child_segment = linked_child_list.first_segment;
		auto data = GetPrimitiveData<char>(child_segment);
		str.append(data, child_segment->count);
		linked_child_list.first_segment = child_segment->next;
	}
	linked_child_list.last_segment = nullptr;

	// use length and (reconstructed) offset to get the correct substrings
	auto aggr_vector_data = FlatVector::GetData(result);
	auto str_length_data = GetListLengthData(segment);

	// get the substrings and write them to the result vector
	idx_t offset = 0;
	for (idx_t i = 0; i < segment->count; i++) {
		if (!null_mask[i]) {
			auto str_length = Load<uint64_t>((data_ptr_t)(str_length_data + i));
			auto substr = str.substr(offset, str_length);
			auto str_t = StringVector::AddStringOrBlob(result, substr);
			((string_t *)aggr_vector_data)[total_count + i] = str_t;
			offset += str_length;
		}
	}
}

static void ReadDataFromListSegment(ReadDataFromSegment &read_data_from_segment, ListSegment *segment, Vector &result,
                                    idx_t &total_count) {

	auto &aggr_vector_validity = FlatVector::Validity(result);

	// set NULLs
	auto null_mask = GetNullMask(segment);
	for (idx_t i = 0; i < segment->count; i++) {
		if (null_mask[i]) {
			aggr_vector_validity.SetInvalid(total_count + i);
		}
	}

	auto list_vector_data = FlatVector::GetData<list_entry_t>(result);

	// get the starting offset
	idx_t offset = 0;
	if (total_count != 0) {
		offset = list_vector_data[total_count - 1].offset + list_vector_data[total_count - 1].length;
	}
	idx_t starting_offset = offset;

	// set length and offsets
	auto list_length_data = GetListLengthData(segment);
	for (idx_t i = 0; i < segment->count; i++) {
		auto list_length = Load<uint64_t>((data_ptr_t)(list_length_data + i));
		list_vector_data[total_count + i].length = list_length;
		list_vector_data[total_count + i].offset = offset;
		offset += list_length;
	}

	auto &child_vector = ListVector::GetEntry(result);
	auto linked_child_list = Load<LinkedList>((data_ptr_t)GetListChildData(segment));
	ListVector::Reserve(result, offset);

	// recurse into the linked list of child values
	D_ASSERT(read_data_from_segment.child_functions.size() == 1);
	BuildListVector(read_data_from_segment.child_functions[0], &linked_child_list, child_vector, starting_offset);
}

static void ReadDataFromStructSegment(ReadDataFromSegment &read_data_from_segment, ListSegment *segment, Vector &result,
                                      idx_t &total_count) {

	auto &aggr_vector_validity = FlatVector::Validity(result);

	// set NULLs
	auto null_mask = GetNullMask(segment);
	for (idx_t i = 0; i < segment->count; i++) {
		if (null_mask[i]) {
			aggr_vector_validity.SetInvalid(total_count + i);
		}
	}

	auto &children = StructVector::GetEntries(result);

	// recurse into the child segments of each child of the struct
	D_ASSERT(children.size() == read_data_from_segment.child_functions.size());
	auto struct_children = GetStructData(segment);
	for (idx_t child_count = 0; child_count < children.size(); child_count++) {
		auto struct_children_segment = Load<ListSegment *>((data_ptr_t)(struct_children + child_count));
		auto &child_function = read_data_from_segment.child_functions[child_count];
		child_function.segment_function(child_function, struct_children_segment, *children[child_count], total_count);
	}
}

static void BuildListVector(ReadDataFromSegment &read_data_from_segment, LinkedList *linked_list, Vector &result,
                            idx_t &initial_total_count) {

	idx_t total_count = initial_total_count;
	while (linked_list->first_segment) {
		auto segment = linked_list->first_segment;
		read_data_from_segment.segment_function(read_data_from_segment, segment, result, total_count);

		total_count += segment->count;
		linked_list->first_segment = segment->next;
	}

	linked_list->last_segment = nullptr;
}

static void InitializeValidities(Vector &vector, idx_t &capacity) {

	auto &validity_mask = FlatVector::Validity(vector);
	validity_mask.Initialize(capacity);

	auto internal_type = vector.GetType().InternalType();
	if (internal_type == PhysicalType::LIST) {
		auto &child_vector = ListVector::GetEntry(vector);
		InitializeValidities(child_vector, capacity);
	} else if (internal_type == PhysicalType::STRUCT) {
		auto &children = StructVector::GetEntries(vector);
		for (auto &child : children) {
			InitializeValidities(*child, capacity);
		}
	}
}

static void RecursiveFlatten(Vector &vector, idx_t &count) {

	if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
		vector.Flatten(count);
	}

	auto internal_type = vector.GetType().InternalType();
	if (internal_type == PhysicalType::LIST) {
		auto &child_vector = ListVector::GetEntry(vector);
		auto child_vector_count = ListVector::GetListSize(vector);
		RecursiveFlatten(child_vector, child_vector_count);
	} else if (internal_type == PhysicalType::STRUCT) {
		auto &children = StructVector::GetEntries(vector);
		for (auto &child : children) {
			RecursiveFlatten(*child, count);
		}
	}
}

struct ListBindData : public FunctionData {
	explicit ListBindData(const LogicalType &stype_p);
	~ListBindData() override;

	LogicalType stype;
	WriteDataToSegment write_data_to_segment;
	ReadDataFromSegment read_data_from_segment;

	unique_ptr<FunctionData> Copy() const override {
		return make_unique<ListBindData>(stype);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const ListBindData &)other_p;
		return stype == other.stype;
	}
};

static void GetSegmentDataFunctions(WriteDataToSegment &write_data_to_segment,
                                    ReadDataFromSegment &read_data_from_segment, const LogicalType &type) {

	auto physical_type = type.InternalType();
	switch (physical_type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<bool>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<bool>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<bool>;
		break;
	}
	case PhysicalType::INT8: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<int8_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<int8_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<int8_t>;
		break;
	}
	case PhysicalType::INT16: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<int16_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<int16_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<int16_t>;
		break;
	}
	case PhysicalType::INT32: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<int32_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<int32_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<int32_t>;
		break;
	}
	case PhysicalType::INT64: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<int64_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<int64_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<int64_t>;
		break;
	}
	case PhysicalType::UINT8: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<uint8_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<uint8_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<uint8_t>;
		break;
	}
	case PhysicalType::UINT16: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<uint16_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<uint16_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<uint16_t>;
		break;
	}
	case PhysicalType::UINT32: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<uint32_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<uint32_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<uint32_t>;
		break;
	}
	case PhysicalType::UINT64: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<uint64_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<uint64_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<uint64_t>;
		break;
	}
	case PhysicalType::FLOAT: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<float>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<float>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<float>;
		break;
	}
	case PhysicalType::DOUBLE: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<double>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<double>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<double>;
		break;
	}
	case PhysicalType::INT128: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<hugeint_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<hugeint_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<hugeint_t>;
		break;
	}
	case PhysicalType::INTERVAL: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<interval_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<interval_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<interval_t>;
		break;
	}
	case PhysicalType::VARCHAR: {
		write_data_to_segment.create_segment = CreateListSegment;
		write_data_to_segment.segment_function = WriteDataToVarcharSegment;
		read_data_from_segment.segment_function = ReadDataFromVarcharSegment;

		write_data_to_segment.child_functions.emplace_back(WriteDataToSegment());
		write_data_to_segment.child_functions.back().create_segment = CreatePrimitiveSegment<char>;
		break;
	}
	case PhysicalType::LIST: {
		write_data_to_segment.create_segment = CreateListSegment;
		write_data_to_segment.segment_function = WriteDataToListSegment;
		read_data_from_segment.segment_function = ReadDataFromListSegment;

		// recurse
		write_data_to_segment.child_functions.emplace_back(WriteDataToSegment());
		read_data_from_segment.child_functions.emplace_back(ReadDataFromSegment());
		GetSegmentDataFunctions(write_data_to_segment.child_functions.back(),
		                        read_data_from_segment.child_functions.back(), ListType::GetChildType(type));
		break;
	}
	case PhysicalType::STRUCT: {
		write_data_to_segment.create_segment = CreateStructSegment;
		write_data_to_segment.segment_function = WriteDataToStructSegment;
		read_data_from_segment.segment_function = ReadDataFromStructSegment;

		// recurse
		auto child_types = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < child_types.size(); i++) {
			write_data_to_segment.child_functions.emplace_back(WriteDataToSegment());
			read_data_from_segment.child_functions.emplace_back(ReadDataFromSegment());
			GetSegmentDataFunctions(write_data_to_segment.child_functions.back(),
			                        read_data_from_segment.child_functions.back(), child_types[i].second);
		}
		break;
	}
	default:
		throw InternalException("LIST aggregate not yet implemented for " + type.ToString());
	}
}

ListBindData::ListBindData(const LogicalType &stype_p) : stype(stype_p) {

	// always unnest once because the result vector is of type LIST
	auto type = ListType::GetChildType(stype_p);
	GetSegmentDataFunctions(write_data_to_segment, read_data_from_segment, type);
}

ListBindData::~ListBindData() {
}

struct ListAggState {
	LinkedList *linked_list;
	LogicalType *type;
	vector<AllocatedData> *owning_vector;
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
			state->linked_list = nullptr;
		}
		if (state->type) {
			delete state->type;
			state->type = nullptr;
		}
		if (state->owning_vector) {
			state->owning_vector->clear();
			delete state->owning_vector;
			state->owning_vector = nullptr;
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
	RecursiveFlatten(input, count);

	auto &list_bind_data = (ListBindData &)*aggr_input_data.bind_data;

	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		if (!state->linked_list) {
			state->linked_list = new LinkedList(0, nullptr, nullptr);
			state->type = new LogicalType(input.GetType());
			state->owning_vector = new vector<AllocatedData>;
		}
		D_ASSERT(state->type);
		AppendRow(list_bind_data.write_data_to_segment, aggr_input_data.allocator, *state->owning_vector,
		          state->linked_list, input, i, count);
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
			combined_ptr[i]->owning_vector = new vector<AllocatedData>;

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

static void ListFinalize(Vector &state_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                         idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (ListAggState **)sdata.data;

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	auto &mask = FlatVector::Validity(result);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	size_t total_len = ListVector::GetListSize(result);

	auto &list_bind_data = (ListBindData &)*aggr_input_data.bind_data;

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
		// FIXME: this is a workaround because the constructor of a vector does not set the size
		// of the validity mask, and by default it is set to STANDARD_VECTOR_SIZE
		// ListVector::Reserve only increases the validity mask, if (to_reserve > capacity),
		// which will not be the case if the value passed to the constructor of aggr_vector
		// is greater than to_reserve
		InitializeValidities(aggr_vector, total_capacity);

		idx_t total_count = 0;
		BuildListVector(list_bind_data.read_data_from_segment, state->linked_list, aggr_vector, total_count);
		ListVector::Append(result, aggr_vector, total_capacity);

		// now destroy the state (for parallel destruction)
		ListFunction::Destroy<ListAggState>(state);
	}
}

unique_ptr<FunctionData> ListBindFunction(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 1);
	D_ASSERT(function.arguments.size() == 1);

	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		function.arguments[0] = LogicalTypeId::UNKNOWN;
		function.return_type = LogicalType::SQLNULL;
		return nullptr;
	}

	function.return_type = LogicalType::LIST(arguments[0]->return_type);
	return make_unique<ListBindData>(function.return_type);
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
