#include "duckdb/common/types/list_segment.hpp"

namespace duckdb {

// forward declarations

template <class T>
static data_ptr_t AllocatePrimitiveData(Allocator &allocator, vector<AllocatedData> &owning_vector,
                                        const uint16_t &capacity) {

	owning_vector.emplace_back(allocator.Allocate(sizeof(ListSegment) + capacity * (sizeof(bool) + sizeof(T))));
	return owning_vector.back().get();
}

static data_ptr_t AllocateListData(Allocator &allocator, vector<AllocatedData> &owning_vector,
                                   const uint16_t &capacity) {

	owning_vector.emplace_back(
	    allocator.Allocate(sizeof(ListSegment) + capacity * (sizeof(bool) + sizeof(uint64_t)) + sizeof(LinkedList)));
	return owning_vector.back().get();
}

static data_ptr_t AllocateStructData(Allocator &allocator, vector<AllocatedData> &owning_vector,
                                     const uint16_t &capacity, const idx_t &child_count) {

	owning_vector.emplace_back(
	    allocator.Allocate(sizeof(ListSegment) + capacity * sizeof(bool) + child_count * sizeof(ListSegment *)));
	return owning_vector.back().get();
}

template <class T>
static T *GetPrimitiveData(const ListSegment *segment) {
	return (T *)(((char *)segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

static uint64_t *GetListLengthData(const ListSegment *segment) {
	return (uint64_t *)(((char *)segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

static LinkedList *GetListChildData(const ListSegment *segment) {
	return (LinkedList *)(((char *)segment) + sizeof(ListSegment) +
	                      segment->capacity * (sizeof(bool) + sizeof(uint64_t)));
}

static ListSegment **GetStructData(const ListSegment *segment) {
	return (ListSegment **)(((char *)segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

static bool *GetNullMask(const ListSegment *segment) {
	return (bool *)(((char *)segment) + sizeof(ListSegment));
}

static uint16_t GetCapacityForNewSegment(const LinkedList *linked_list) {

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
                                           vector<AllocatedData> &owning_vector, const uint16_t &capacity) {

	// allocate data and set the header
	auto segment = (ListSegment *)AllocatePrimitiveData<T>(allocator, owning_vector, capacity);
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;
	return segment;
}

static ListSegment *CreateListSegment(WriteDataToSegment &, Allocator &allocator, vector<AllocatedData> &owning_vector,
                                      const uint16_t &capacity) {

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
                                        vector<AllocatedData> &owning_vector, const uint16_t &capacity) {

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
			write_data_to_segment.child_functions[0].AppendRow(allocator, owning_vector, &child_segments, child_vector,
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

void WriteDataToSegment::AppendRow(Allocator &allocator, vector<AllocatedData> &owning_vector, LinkedList *linked_list,
                                   Vector &input, idx_t &entry_idx, idx_t &count) {

	D_ASSERT(input.GetVectorType() == VectorType::FLAT_VECTOR);
	auto &write_data_to_segment = *this;
	auto segment = GetSegment(write_data_to_segment, allocator, owning_vector, linked_list);
	write_data_to_segment.segment_function(write_data_to_segment, allocator, owning_vector, segment, input, entry_idx,
	                                       count);

	linked_list->total_capacity++;
	segment->count++;
}

template <class T>
static void ReadDataFromPrimitiveSegment(ReadDataFromSegment &, const ListSegment *segment, Vector &result,
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

static void ReadDataFromVarcharSegment(ReadDataFromSegment &, const ListSegment *segment, Vector &result,
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

static void ReadDataFromListSegment(ReadDataFromSegment &read_data_from_segment, const ListSegment *segment,
                                    Vector &result, idx_t &total_count) {

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
	read_data_from_segment.child_functions[0].BuildListVector(&linked_child_list, child_vector, starting_offset);
}

static void ReadDataFromStructSegment(ReadDataFromSegment &read_data_from_segment, const ListSegment *segment,
                                      Vector &result, idx_t &total_count) {

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

void ReadDataFromSegment::BuildListVector(LinkedList *linked_list, Vector &result, idx_t &initial_total_count) {
	auto &read_data_from_segment = *this;
	idx_t total_count = initial_total_count;
	while (linked_list->first_segment) {
		auto segment = linked_list->first_segment;
		read_data_from_segment.segment_function(read_data_from_segment, segment, result, total_count);

		total_count += segment->count;
		linked_list->first_segment = segment->next;
	}

	linked_list->last_segment = nullptr;
}

template <class T>
static ListSegment *CopyDataFromPrimitiveSegment(CopyDataFromSegment &, const ListSegment *source, Allocator &allocator,
                                                 vector<AllocatedData> &owning_vector) {

	auto target = (ListSegment *)AllocatePrimitiveData<T>(allocator, owning_vector, source->capacity);
	memcpy(target, source, sizeof(ListSegment) + source->capacity * (sizeof(bool) + sizeof(T)));
	target->next = nullptr;
	return target;
}

static ListSegment *CopyDataFromListSegment(CopyDataFromSegment &copy_data_from_segment, const ListSegment *source,
                                            Allocator &allocator, vector<AllocatedData> &owning_vector) {

	// create an empty linked list for the child vector of target
	auto source_linked_child_list = Load<LinkedList>((data_ptr_t)GetListChildData(source));

	// create the segment
	auto target = (ListSegment *)AllocateListData(allocator, owning_vector, source->capacity);
	memcpy(target, source,
	       sizeof(ListSegment) + source->capacity * (sizeof(bool) + sizeof(uint64_t)) + sizeof(LinkedList));
	target->next = nullptr;

	auto target_linked_list = GetListChildData(target);
	LinkedList linked_list(source_linked_child_list.total_capacity, nullptr, nullptr);
	Store<LinkedList>(linked_list, (data_ptr_t)target_linked_list);

	// recurse to copy the linked child list
	auto target_linked_child_list = Load<LinkedList>((data_ptr_t)GetListChildData(target));
	D_ASSERT(copy_data_from_segment.child_functions.size() == 1);
	copy_data_from_segment.child_functions[0].CopyLinkedList(&source_linked_child_list, target_linked_child_list,
	                                                         allocator, owning_vector);

	// store the updated linked list
	Store<LinkedList>(target_linked_child_list, (data_ptr_t)GetListChildData(target));
	return target;
}

static ListSegment *CopyDataFromStructSegment(CopyDataFromSegment &copy_data_from_segment, const ListSegment *source,
                                              Allocator &allocator, vector<AllocatedData> &owning_vector) {

	auto source_child_count = copy_data_from_segment.child_functions.size();
	auto target = (ListSegment *)AllocateStructData(allocator, owning_vector, source->capacity, source_child_count);
	memcpy(target, source,
	       sizeof(ListSegment) + source->capacity * sizeof(bool) + source_child_count * sizeof(ListSegment *));
	target->next = nullptr;

	// recurse and copy the children
	auto source_child_segments = GetStructData(source);
	auto target_child_segments = GetStructData(target);

	for (idx_t i = 0; i < copy_data_from_segment.child_functions.size(); i++) {
		auto child_function = copy_data_from_segment.child_functions[i];
		auto source_child_segment = Load<ListSegment *>((data_ptr_t)(source_child_segments + i));
		auto target_child_segment =
		    child_function.segment_function(child_function, source_child_segment, allocator, owning_vector);
		Store<ListSegment *>(target_child_segment, (data_ptr_t)(target_child_segments + i));
	}
	return target;
}

void CopyDataFromSegment::CopyLinkedList(const LinkedList *source_list, LinkedList &target_list, Allocator &allocator,
                                         vector<AllocatedData> &owning_vector) {
	auto &copy_data_from_segment = *this;
	auto source_segment = source_list->first_segment;

	while (source_segment) {
		auto target_segment =
		    copy_data_from_segment.segment_function(copy_data_from_segment, source_segment, allocator, owning_vector);
		source_segment = source_segment->next;

		if (!target_list.first_segment) {
			target_list.first_segment = target_segment;
		}
		if (target_list.last_segment) {
			target_list.last_segment->next = target_segment;
		}
		target_list.last_segment = target_segment;
	}
}

void GetSegmentDataFunctions(WriteDataToSegment &write_data_to_segment, ReadDataFromSegment &read_data_from_segment,
                             CopyDataFromSegment &copy_data_from_segment, const LogicalType &type) {

	auto physical_type = type.InternalType();
	switch (physical_type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<bool>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<bool>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<bool>;
		copy_data_from_segment.segment_function = CopyDataFromPrimitiveSegment<bool>;
		break;
	}
	case PhysicalType::INT8: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<int8_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<int8_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<int8_t>;
		copy_data_from_segment.segment_function = CopyDataFromPrimitiveSegment<int8_t>;
		break;
	}
	case PhysicalType::INT16: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<int16_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<int16_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<int16_t>;
		copy_data_from_segment.segment_function = CopyDataFromPrimitiveSegment<int16_t>;
		break;
	}
	case PhysicalType::INT32: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<int32_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<int32_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<int32_t>;
		copy_data_from_segment.segment_function = CopyDataFromPrimitiveSegment<int32_t>;
		break;
	}
	case PhysicalType::INT64: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<int64_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<int64_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<int64_t>;
		copy_data_from_segment.segment_function = CopyDataFromPrimitiveSegment<int64_t>;
		break;
	}
	case PhysicalType::UINT8: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<uint8_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<uint8_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<uint8_t>;
		copy_data_from_segment.segment_function = CopyDataFromPrimitiveSegment<uint8_t>;
		break;
	}
	case PhysicalType::UINT16: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<uint16_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<uint16_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<uint16_t>;
		copy_data_from_segment.segment_function = CopyDataFromPrimitiveSegment<uint16_t>;
		break;
	}
	case PhysicalType::UINT32: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<uint32_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<uint32_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<uint32_t>;
		copy_data_from_segment.segment_function = CopyDataFromPrimitiveSegment<uint32_t>;
		break;
	}
	case PhysicalType::UINT64: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<uint64_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<uint64_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<uint64_t>;
		copy_data_from_segment.segment_function = CopyDataFromPrimitiveSegment<uint64_t>;
		break;
	}
	case PhysicalType::FLOAT: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<float>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<float>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<float>;
		copy_data_from_segment.segment_function = CopyDataFromPrimitiveSegment<float>;
		break;
	}
	case PhysicalType::DOUBLE: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<double>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<double>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<double>;
		copy_data_from_segment.segment_function = CopyDataFromPrimitiveSegment<double>;
		break;
	}
	case PhysicalType::INT128: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<hugeint_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<hugeint_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<hugeint_t>;
		copy_data_from_segment.segment_function = CopyDataFromPrimitiveSegment<hugeint_t>;
		break;
	}
	case PhysicalType::INTERVAL: {
		write_data_to_segment.create_segment = CreatePrimitiveSegment<interval_t>;
		write_data_to_segment.segment_function = WriteDataToPrimitiveSegment<interval_t>;
		read_data_from_segment.segment_function = ReadDataFromPrimitiveSegment<interval_t>;
		copy_data_from_segment.segment_function = CopyDataFromPrimitiveSegment<interval_t>;
		break;
	}
	case PhysicalType::VARCHAR: {
		write_data_to_segment.create_segment = CreateListSegment;
		write_data_to_segment.segment_function = WriteDataToVarcharSegment;
		read_data_from_segment.segment_function = ReadDataFromVarcharSegment;
		copy_data_from_segment.segment_function = CopyDataFromListSegment;

		write_data_to_segment.child_functions.emplace_back();
		write_data_to_segment.child_functions.back().create_segment = CreatePrimitiveSegment<char>;
		copy_data_from_segment.child_functions.emplace_back();
		copy_data_from_segment.child_functions.back().segment_function = CopyDataFromPrimitiveSegment<char>;
		break;
	}
	case PhysicalType::LIST: {
		write_data_to_segment.create_segment = CreateListSegment;
		write_data_to_segment.segment_function = WriteDataToListSegment;
		read_data_from_segment.segment_function = ReadDataFromListSegment;
		copy_data_from_segment.segment_function = CopyDataFromListSegment;

		// recurse
		write_data_to_segment.child_functions.emplace_back();
		read_data_from_segment.child_functions.emplace_back();
		copy_data_from_segment.child_functions.emplace_back();
		GetSegmentDataFunctions(write_data_to_segment.child_functions.back(),
		                        read_data_from_segment.child_functions.back(),
		                        copy_data_from_segment.child_functions.back(), ListType::GetChildType(type));
		break;
	}
	case PhysicalType::STRUCT: {
		write_data_to_segment.create_segment = CreateStructSegment;
		write_data_to_segment.segment_function = WriteDataToStructSegment;
		read_data_from_segment.segment_function = ReadDataFromStructSegment;
		copy_data_from_segment.segment_function = CopyDataFromStructSegment;

		// recurse
		auto child_types = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < child_types.size(); i++) {
			write_data_to_segment.child_functions.emplace_back();
			read_data_from_segment.child_functions.emplace_back();
			copy_data_from_segment.child_functions.emplace_back();
			GetSegmentDataFunctions(write_data_to_segment.child_functions.back(),
			                        read_data_from_segment.child_functions.back(),
			                        copy_data_from_segment.child_functions.back(), child_types[i].second);
		}
		break;
	}
	default:
		throw InternalException("LIST aggregate not yet implemented for " + type.ToString());
	}
}

} // namespace duckdb
