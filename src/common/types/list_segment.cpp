#include "duckdb/common/types/list_segment.hpp"

namespace duckdb {

// forward declarations
//===--------------------------------------------------------------------===//
// Primitives
//===--------------------------------------------------------------------===//
template <class T>
static idx_t GetAllocationSize(uint16_t capacity) {
	return AlignValue(sizeof(ListSegment) + capacity * (sizeof(bool) + sizeof(T)));
}

template <class T>
static data_ptr_t AllocatePrimitiveData(ArenaAllocator &allocator, uint16_t capacity) {
	return allocator.Allocate(GetAllocationSize<T>(capacity));
}

template <class T>
static T *GetPrimitiveData(ListSegment *segment) {
	return reinterpret_cast<T *>(data_ptr_cast(segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

template <class T>
static const T *GetPrimitiveData(const ListSegment *segment) {
	return reinterpret_cast<const T *>(const_data_ptr_cast(segment) + sizeof(ListSegment) +
	                                   segment->capacity * sizeof(bool));
}

//===--------------------------------------------------------------------===//
// Lists
//===--------------------------------------------------------------------===//
static idx_t GetAllocationSizeList(uint16_t capacity) {
	return AlignValue(sizeof(ListSegment) + capacity * (sizeof(bool) + sizeof(uint64_t)) + sizeof(LinkedList));
}

static data_ptr_t AllocateListData(ArenaAllocator &allocator, uint16_t capacity) {
	return allocator.Allocate(GetAllocationSizeList(capacity));
}

static uint64_t *GetListLengthData(ListSegment *segment) {
	return reinterpret_cast<uint64_t *>(data_ptr_cast(segment) + sizeof(ListSegment) +
	                                    segment->capacity * sizeof(bool));
}

static const uint64_t *GetListLengthData(const ListSegment *segment) {
	return reinterpret_cast<const uint64_t *>(const_data_ptr_cast(segment) + sizeof(ListSegment) +
	                                          segment->capacity * sizeof(bool));
}

static const LinkedList *GetListChildData(const ListSegment *segment) {
	return reinterpret_cast<const LinkedList *>(const_data_ptr_cast(segment) + sizeof(ListSegment) +
	                                            segment->capacity * (sizeof(bool) + sizeof(uint64_t)));
}

static LinkedList *GetListChildData(ListSegment *segment) {
	return reinterpret_cast<LinkedList *>(data_ptr_cast(segment) + sizeof(ListSegment) +
	                                      segment->capacity * (sizeof(bool) + sizeof(uint64_t)));
}

//===--------------------------------------------------------------------===//
// Structs
//===--------------------------------------------------------------------===//
static idx_t GetAllocationSizeStruct(uint16_t capacity, idx_t child_count) {
	return AlignValue(sizeof(ListSegment) + capacity * sizeof(bool) + child_count * sizeof(ListSegment *));
}

static data_ptr_t AllocateStructData(ArenaAllocator &allocator, uint16_t capacity, idx_t child_count) {
	return allocator.Allocate(GetAllocationSizeStruct(capacity, child_count));
}

static ListSegment **GetStructData(ListSegment *segment) {
	return reinterpret_cast<ListSegment **>(data_ptr_cast(segment) + +sizeof(ListSegment) +
	                                        segment->capacity * sizeof(bool));
}

static const ListSegment *const *GetStructData(const ListSegment *segment) {
	return reinterpret_cast<const ListSegment *const *>(const_data_ptr_cast(segment) + sizeof(ListSegment) +
	                                                    segment->capacity * sizeof(bool));
}

static bool *GetNullMask(ListSegment *segment) {
	return reinterpret_cast<bool *>(data_ptr_cast(segment) + sizeof(ListSegment));
}

static const bool *GetNullMask(const ListSegment *segment) {
	return reinterpret_cast<const bool *>(const_data_ptr_cast(segment) + sizeof(ListSegment));
}

static uint16_t GetCapacityForNewSegment(uint16_t capacity) {
	auto next_power_of_two = idx_t(capacity) * 2;
	if (next_power_of_two >= NumericLimits<uint16_t>::Maximum()) {
		return capacity;
	}
	return uint16_t(next_power_of_two);
}

//===--------------------------------------------------------------------===//
// Create & Destroy
//===--------------------------------------------------------------------===//
template <class T>
static ListSegment *CreatePrimitiveSegment(const ListSegmentFunctions &, ArenaAllocator &allocator, uint16_t capacity) {
	// allocate data and set the header
	auto segment = (ListSegment *)AllocatePrimitiveData<T>(allocator, capacity);
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;
	return segment;
}

static ListSegment *CreateListSegment(const ListSegmentFunctions &, ArenaAllocator &allocator, uint16_t capacity) {
	// allocate data and set the header
	auto segment = reinterpret_cast<ListSegment *>(AllocateListData(allocator, capacity));
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;

	// create an empty linked list for the child vector
	auto linked_child_list = GetListChildData(segment);
	LinkedList linked_list(0, nullptr, nullptr);
	Store<LinkedList>(linked_list, data_ptr_cast(linked_child_list));

	return segment;
}

static ListSegment *CreateStructSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                        uint16_t capacity) {
	// allocate data and set header
	auto segment =
	    reinterpret_cast<ListSegment *>(AllocateStructData(allocator, capacity, functions.child_functions.size()));
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;

	// create a child ListSegment with exactly the same capacity for each child vector
	auto child_segments = GetStructData(segment);
	for (idx_t i = 0; i < functions.child_functions.size(); i++) {
		auto child_function = functions.child_functions[i];
		auto child_segment = child_function.create_segment(child_function, allocator, capacity);
		Store<ListSegment *>(child_segment, data_ptr_cast(child_segments + i));
	}

	return segment;
}

static ListSegment *GetSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                               LinkedList &linked_list) {
	ListSegment *segment;

	// determine segment
	if (!linked_list.last_segment) {
		// empty linked list, create the first (and last) segment
		auto capacity = ListSegment::INITIAL_CAPACITY;
		segment = functions.create_segment(functions, allocator, capacity);
		linked_list.first_segment = segment;
		linked_list.last_segment = segment;

	} else if (linked_list.last_segment->capacity == linked_list.last_segment->count) {
		// the last segment of the linked list is full, create a new one and append it
		auto capacity = GetCapacityForNewSegment(linked_list.last_segment->capacity);
		segment = functions.create_segment(functions, allocator, capacity);
		linked_list.last_segment->next = segment;
		linked_list.last_segment = segment;
	} else {
		// the last segment of the linked list is not full, append the data to it
		segment = linked_list.last_segment;
	}

	D_ASSERT(segment);
	return segment;
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
template <class T>
static void WriteDataToPrimitiveSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                        ListSegment *segment, Vector &input, idx_t &entry_idx, idx_t &count) {

	// get the vector data and the source index of the entry that we want to write
	auto input_data = FlatVector::GetData(input);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto is_null = FlatVector::IsNull(input, entry_idx);
	null_mask[segment->count] = is_null;

	// write value
	if (!is_null) {
		auto data = GetPrimitiveData<T>(segment);
		Store<T>(((T *)input_data)[entry_idx], data_ptr_cast(data + segment->count));
	}
}

static void WriteDataToVarcharSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                      ListSegment *segment, Vector &input, idx_t &entry_idx, idx_t &count) {

	// get the vector data and the source index of the entry that we want to write
	auto input_data = FlatVector::GetData<string_t>(input);

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
		str_t = input_data[entry_idx];
		str_length = str_t.GetSize();
	}

	// we can reconstruct the offset from the length
	Store<uint64_t>(str_length, data_ptr_cast(str_length_data + segment->count));

	if (is_null) {
		return;
	}

	// write the characters to the linked list of child segments
	auto child_segments = Load<LinkedList>(data_ptr_cast(GetListChildData(segment)));
	for (char &c : str_t.GetString()) {
		auto child_segment = GetSegment(functions.child_functions.back(), allocator, child_segments);
		auto data = GetPrimitiveData<char>(child_segment);
		data[child_segment->count] = c;
		child_segment->count++;
		child_segments.total_capacity++;
	}

	// store the updated linked list
	Store<LinkedList>(child_segments, data_ptr_cast(GetListChildData(segment)));
}

static void WriteDataToListSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                   ListSegment *segment, Vector &input, idx_t &entry_idx, idx_t &count) {

	// get the vector data and the source index of the entry that we want to write
	auto input_data = FlatVector::GetData<list_entry_t>(input);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto is_null = FlatVector::IsNull(input, entry_idx);
	null_mask[segment->count] = is_null;

	// set the length of this list
	auto list_length_data = GetListLengthData(segment);
	uint64_t list_length = 0;

	if (!is_null) {
		// get list entry information
		auto list_entries = input_data;
		const auto &list_entry = list_entries[entry_idx];
		list_length = list_entry.length;

		// get the child vector and its data
		auto lists_size = ListVector::GetListSize(input);
		auto &child_vector = ListVector::GetEntry(input);

		// loop over the child vector entries and recurse on them
		auto child_segments = Load<LinkedList>(data_ptr_cast(GetListChildData(segment)));
		D_ASSERT(functions.child_functions.size() == 1);
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
			auto source_idx_child = list_entry.offset + child_idx;
			functions.child_functions[0].AppendRow(allocator, child_segments, child_vector, source_idx_child,
			                                       lists_size);
		}
		// store the updated linked list
		Store<LinkedList>(child_segments, data_ptr_cast(GetListChildData(segment)));
	}

	Store<uint64_t>(list_length, data_ptr_cast(list_length_data + segment->count));
}

static void WriteDataToStructSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                     ListSegment *segment, Vector &input, idx_t &entry_idx, idx_t &count) {

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto is_null = FlatVector::IsNull(input, entry_idx);
	null_mask[segment->count] = is_null;

	// write value
	auto &children = StructVector::GetEntries(input);
	D_ASSERT(children.size() == functions.child_functions.size());
	auto child_list = GetStructData(segment);

	// write the data of each of the children of the struct
	for (idx_t child_count = 0; child_count < children.size(); child_count++) {
		auto child_list_segment = Load<ListSegment *>(data_ptr_cast(child_list + child_count));
		auto &child_function = functions.child_functions[child_count];
		child_function.write_data(child_function, allocator, child_list_segment, *children[child_count], entry_idx,
		                          count);
		child_list_segment->count++;
	}
}

void ListSegmentFunctions::AppendRow(ArenaAllocator &allocator, LinkedList &linked_list, Vector &input,
                                     idx_t &entry_idx, idx_t &count) const {

	D_ASSERT(input.GetVectorType() == VectorType::FLAT_VECTOR);
	auto &write_data_to_segment = *this;
	auto segment = GetSegment(write_data_to_segment, allocator, linked_list);
	write_data_to_segment.write_data(write_data_to_segment, allocator, segment, input, entry_idx, count);

	linked_list.total_capacity++;
	segment->count++;
}

//===--------------------------------------------------------------------===//
// Read
//===--------------------------------------------------------------------===//
template <class T>
static void ReadDataFromPrimitiveSegment(const ListSegmentFunctions &, const ListSegment *segment, Vector &result,
                                         idx_t &total_count) {

	auto &aggr_vector_validity = FlatVector::Validity(result);

	// set NULLs
	auto null_mask = GetNullMask(segment);
	for (idx_t i = 0; i < segment->count; i++) {
		if (null_mask[i]) {
			aggr_vector_validity.SetInvalid(total_count + i);
		}
	}

	auto aggr_vector_data = FlatVector::GetData<T>(result);

	// load values
	for (idx_t i = 0; i < segment->count; i++) {
		if (aggr_vector_validity.RowIsValid(total_count + i)) {
			auto data = GetPrimitiveData<T>(segment);
			aggr_vector_data[total_count + i] = Load<T>(const_data_ptr_cast(data + i));
		}
	}
}

static void ReadDataFromVarcharSegment(const ListSegmentFunctions &, const ListSegment *segment, Vector &result,
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
	auto linked_child_list = Load<LinkedList>(const_data_ptr_cast(GetListChildData(segment)));
	while (linked_child_list.first_segment) {
		auto child_segment = linked_child_list.first_segment;
		auto data = GetPrimitiveData<char>(child_segment);
		str.append(data, child_segment->count);
		linked_child_list.first_segment = child_segment->next;
	}
	linked_child_list.last_segment = nullptr;

	// use length and (reconstructed) offset to get the correct substrings
	auto aggr_vector_data = FlatVector::GetData<string_t>(result);
	auto str_length_data = GetListLengthData(segment);

	// get the substrings and write them to the result vector
	idx_t offset = 0;
	for (idx_t i = 0; i < segment->count; i++) {
		if (!null_mask[i]) {
			auto str_length = Load<uint64_t>(const_data_ptr_cast(str_length_data + i));
			auto substr = str.substr(offset, str_length);
			auto str_t = StringVector::AddStringOrBlob(result, substr);
			aggr_vector_data[total_count + i] = str_t;
			offset += str_length;
		}
	}
}

static void ReadDataFromListSegment(const ListSegmentFunctions &functions, const ListSegment *segment, Vector &result,
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
		auto list_length = Load<uint64_t>(const_data_ptr_cast(list_length_data + i));
		list_vector_data[total_count + i].length = list_length;
		list_vector_data[total_count + i].offset = offset;
		offset += list_length;
	}

	auto &child_vector = ListVector::GetEntry(result);
	auto linked_child_list = Load<LinkedList>(const_data_ptr_cast(GetListChildData(segment)));
	ListVector::Reserve(result, offset);

	// recurse into the linked list of child values
	D_ASSERT(functions.child_functions.size() == 1);
	functions.child_functions[0].BuildListVector(linked_child_list, child_vector, starting_offset);
	ListVector::SetListSize(result, offset);
}

static void ReadDataFromStructSegment(const ListSegmentFunctions &functions, const ListSegment *segment, Vector &result,
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
	D_ASSERT(children.size() == functions.child_functions.size());
	auto struct_children = GetStructData(segment);
	for (idx_t child_count = 0; child_count < children.size(); child_count++) {
		auto struct_children_segment = Load<ListSegment *>(const_data_ptr_cast(struct_children + child_count));
		auto &child_function = functions.child_functions[child_count];
		child_function.read_data(child_function, struct_children_segment, *children[child_count], total_count);
	}
}

void ListSegmentFunctions::BuildListVector(const LinkedList &linked_list, Vector &result,
                                           idx_t &initial_total_count) const {
	auto &read_data_from_segment = *this;
	idx_t total_count = initial_total_count;
	auto segment = linked_list.first_segment;
	while (segment) {
		read_data_from_segment.read_data(read_data_from_segment, segment, result, total_count);

		total_count += segment->count;
		segment = segment->next;
	}
}

//===--------------------------------------------------------------------===//
// Copy
//===--------------------------------------------------------------------===//
template <class T>
static ListSegment *CopyDataFromPrimitiveSegment(const ListSegmentFunctions &, const ListSegment *source,
                                                 ArenaAllocator &allocator) {

	auto target = (ListSegment *)AllocatePrimitiveData<T>(allocator, source->capacity);
	memcpy(target, source, sizeof(ListSegment) + source->capacity * (sizeof(bool) + sizeof(T)));
	target->next = nullptr;
	return target;
}

static ListSegment *CopyDataFromListSegment(const ListSegmentFunctions &functions, const ListSegment *source,
                                            ArenaAllocator &allocator) {

	// create an empty linked list for the child vector of target
	auto source_linked_child_list = Load<LinkedList>(const_data_ptr_cast(GetListChildData(source)));

	// create the segment
	auto target = reinterpret_cast<ListSegment *>(AllocateListData(allocator, source->capacity));
	memcpy(target, source,
	       sizeof(ListSegment) + source->capacity * (sizeof(bool) + sizeof(uint64_t)) + sizeof(LinkedList));
	target->next = nullptr;

	auto target_linked_list = GetListChildData(target);
	LinkedList linked_list(source_linked_child_list.total_capacity, nullptr, nullptr);
	Store<LinkedList>(linked_list, data_ptr_cast(target_linked_list));

	// recurse to copy the linked child list
	auto target_linked_child_list = Load<LinkedList>(data_ptr_cast(GetListChildData(target)));
	D_ASSERT(functions.child_functions.size() == 1);
	functions.child_functions[0].CopyLinkedList(source_linked_child_list, target_linked_child_list, allocator);

	// store the updated linked list
	Store<LinkedList>(target_linked_child_list, data_ptr_cast(GetListChildData(target)));
	return target;
}

static ListSegment *CopyDataFromStructSegment(const ListSegmentFunctions &functions, const ListSegment *source,
                                              ArenaAllocator &allocator) {

	auto source_child_count = functions.child_functions.size();
	auto target = reinterpret_cast<ListSegment *>(AllocateStructData(allocator, source->capacity, source_child_count));
	memcpy(target, source,
	       sizeof(ListSegment) + source->capacity * sizeof(bool) + source_child_count * sizeof(ListSegment *));
	target->next = nullptr;

	// recurse and copy the children
	auto source_child_segments = GetStructData(source);
	auto target_child_segments = GetStructData(target);

	for (idx_t i = 0; i < functions.child_functions.size(); i++) {
		auto child_function = functions.child_functions[i];
		auto source_child_segment = Load<ListSegment *>(const_data_ptr_cast(source_child_segments + i));
		auto target_child_segment = child_function.copy_data(child_function, source_child_segment, allocator);
		Store<ListSegment *>(target_child_segment, data_ptr_cast(target_child_segments + i));
	}
	return target;
}

void ListSegmentFunctions::CopyLinkedList(const LinkedList &source_list, LinkedList &target_list,
                                          ArenaAllocator &allocator) const {
	auto &copy_data_from_segment = *this;
	auto source_segment = source_list.first_segment;

	while (source_segment) {
		auto target_segment = copy_data_from_segment.copy_data(copy_data_from_segment, source_segment, allocator);
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

//===--------------------------------------------------------------------===//
// Functions
//===--------------------------------------------------------------------===//
template <class T>
void SegmentPrimitiveFunction(ListSegmentFunctions &functions) {
	functions.create_segment = CreatePrimitiveSegment<T>;
	functions.write_data = WriteDataToPrimitiveSegment<T>;
	functions.read_data = ReadDataFromPrimitiveSegment<T>;
	functions.copy_data = CopyDataFromPrimitiveSegment<T>;
}

void GetSegmentDataFunctions(ListSegmentFunctions &functions, const LogicalType &type) {

	auto physical_type = type.InternalType();
	switch (physical_type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL:
		SegmentPrimitiveFunction<bool>(functions);
		break;
	case PhysicalType::INT8:
		SegmentPrimitiveFunction<int8_t>(functions);
		break;
	case PhysicalType::INT16:
		SegmentPrimitiveFunction<int16_t>(functions);
		break;
	case PhysicalType::INT32:
		SegmentPrimitiveFunction<int32_t>(functions);
		break;
	case PhysicalType::INT64:
		SegmentPrimitiveFunction<int64_t>(functions);
		break;
	case PhysicalType::UINT8:
		SegmentPrimitiveFunction<uint8_t>(functions);
		break;
	case PhysicalType::UINT16:
		SegmentPrimitiveFunction<uint16_t>(functions);
		break;
	case PhysicalType::UINT32:
		SegmentPrimitiveFunction<uint32_t>(functions);
		break;
	case PhysicalType::UINT64:
		SegmentPrimitiveFunction<uint64_t>(functions);
		break;
	case PhysicalType::FLOAT:
		SegmentPrimitiveFunction<float>(functions);
		break;
	case PhysicalType::DOUBLE:
		SegmentPrimitiveFunction<double>(functions);
		break;
	case PhysicalType::INT128:
		SegmentPrimitiveFunction<hugeint_t>(functions);
		break;
	case PhysicalType::INTERVAL:
		SegmentPrimitiveFunction<interval_t>(functions);
		break;
	case PhysicalType::VARCHAR: {
		functions.create_segment = CreateListSegment;
		functions.write_data = WriteDataToVarcharSegment;
		functions.read_data = ReadDataFromVarcharSegment;
		functions.copy_data = CopyDataFromListSegment;

		functions.child_functions.emplace_back();
		SegmentPrimitiveFunction<char>(functions.child_functions.back());
		break;
	}
	case PhysicalType::LIST: {
		functions.create_segment = CreateListSegment;
		functions.write_data = WriteDataToListSegment;
		functions.read_data = ReadDataFromListSegment;
		functions.copy_data = CopyDataFromListSegment;

		// recurse
		functions.child_functions.emplace_back();
		GetSegmentDataFunctions(functions.child_functions.back(), ListType::GetChildType(type));
		break;
	}
	case PhysicalType::STRUCT: {
		functions.create_segment = CreateStructSegment;
		functions.write_data = WriteDataToStructSegment;
		functions.read_data = ReadDataFromStructSegment;
		functions.copy_data = CopyDataFromStructSegment;

		// recurse
		auto child_types = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < child_types.size(); i++) {
			functions.child_functions.emplace_back();
			GetSegmentDataFunctions(functions.child_functions.back(), child_types[i].second);
		}
		break;
	}
	default:
		throw InternalException("LIST aggregate not yet implemented for " + type.ToString());
	}
}

} // namespace duckdb
