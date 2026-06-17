#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/uhugeint.hpp"

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
// Strings
//===--------------------------------------------------------------------===//
static idx_t GetStringAllocationSize(uint16_t capacity) {
	return AlignValue(sizeof(ListSegment) + (capacity * (sizeof(char))));
}

static data_ptr_t AllocateStringData(ArenaAllocator &allocator, uint16_t capacity) {
	return allocator.Allocate(GetStringAllocationSize(capacity));
}

static char *GetStringData(ListSegment *segment) {
	return reinterpret_cast<char *>(data_ptr_cast(segment) + sizeof(ListSegment));
}

static const char *GetStringData(const ListSegment *segment) {
	return reinterpret_cast<const char *>(const_data_ptr_cast(segment) + sizeof(ListSegment));
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
// Array
//===--------------------------------------------------------------------===//
static idx_t GetAllocationSizeArray(uint16_t capacity) {
	// Only store the null mask for the array segment, length is fixed so we don't need to store it
	return AlignValue(sizeof(ListSegment) + capacity * (sizeof(bool)) + sizeof(LinkedList));
}

static data_ptr_t AllocateArrayData(ArenaAllocator &allocator, uint16_t capacity) {
	return allocator.Allocate(GetAllocationSizeArray(capacity));
}

static const LinkedList *GetArrayChildData(const ListSegment *segment) {
	return reinterpret_cast<const LinkedList *>(const_data_ptr_cast(segment) + sizeof(ListSegment) +
	                                            segment->capacity * sizeof(bool));
}

static LinkedList *GetArrayChildData(ListSegment *segment) {
	return reinterpret_cast<LinkedList *>(data_ptr_cast(segment) + sizeof(ListSegment) +
	                                      segment->capacity * sizeof(bool));
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
// Create
//===--------------------------------------------------------------------===//
template <class T>
static ListSegment *CreatePrimitiveSegment(const ListSegmentFunctions &, ArenaAllocator &allocator, uint16_t capacity) {
	// allocate data and set the header
	auto segment = reinterpret_cast<ListSegment *>(AllocatePrimitiveData<T>(allocator, capacity));
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;
	return segment;
}

static ListSegment *CreateVarcharDataSegment(const ListSegmentFunctions &, ArenaAllocator &allocator,
                                             uint16_t capacity) {
	// allocate data and set the header
	auto segment = reinterpret_cast<ListSegment *>(AllocateStringData(allocator, capacity));
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

static ListSegment *CreateArraySegment(const ListSegmentFunctions &, ArenaAllocator &allocator, uint16_t capacity) {
	// allocate data and set header
	auto segment = reinterpret_cast<ListSegment *>(AllocateArrayData(allocator, capacity));

	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;

	// create an empty linked list for the child vector
	auto linked_child_list = GetArrayChildData(segment);
	LinkedList linked_list(0, nullptr, nullptr);
	Store<LinkedList>(linked_list, data_ptr_cast(linked_child_list));

	return segment;
}

static ListSegment *GetSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                               LinkedList &linked_list) {
	ListSegment *segment;

	// determine segment
	if (!linked_list.last_segment) {
		// empty linked list, create the first (and last) segment
		segment = functions.create_segment(functions, allocator, functions.initial_capacity);
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
static void WriteDataToPrimitiveSegment(const ListSegmentFunctions &, ArenaAllocator &, ListSegment *segment,
                                        RecursiveUnifiedVectorFormat &input_data, idx_t &entry_idx) {
	auto sel_entry_idx = input_data.unified.sel->get_index(entry_idx);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto valid = input_data.unified.validity.RowIsValid(sel_entry_idx);
	null_mask[segment->count] = !valid;

	// write value
	if (valid) {
		auto segment_data = GetPrimitiveData<T>(segment);
		auto input_data_ptr = UnifiedVectorFormat::GetData<T>(input_data.unified);
		Store<T>(input_data_ptr[sel_entry_idx], data_ptr_cast(segment_data + segment->count));
	}
}

static void WriteDataToVarcharSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                      ListSegment *segment, RecursiveUnifiedVectorFormat &input_data,
                                      idx_t &entry_idx) {
	auto sel_entry_idx = input_data.unified.sel->get_index(entry_idx);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto valid = input_data.unified.validity.RowIsValid(sel_entry_idx);
	null_mask[segment->count] = !valid;

	// set the length of this string
	auto str_length_data = GetListLengthData(segment);

	// we can reconstruct the offset from the length
	if (!valid) {
		Store<uint64_t>(0, data_ptr_cast(str_length_data + segment->count));
		return;
	}
	auto &str_entry = UnifiedVectorFormat::GetData<string_t>(input_data.unified)[sel_entry_idx];
	auto str_data = str_entry.GetData();
	idx_t str_size = str_entry.GetSize();
	Store<uint64_t>(str_size, data_ptr_cast(str_length_data + segment->count));

	// write the characters to the linked list of child segments
	auto child_segments = Load<LinkedList>(data_ptr_cast(GetListChildData(segment)));
	idx_t current_offset = 0;
	while (current_offset < str_size) {
		auto child_segment = GetSegment(functions.child_functions.back(), allocator, child_segments);
		auto data = GetStringData(child_segment);
		idx_t copy_count = MinValue<idx_t>(str_size - current_offset, child_segment->capacity - child_segment->count);
		memcpy(data + child_segment->count, str_data + current_offset, copy_count);
		current_offset += copy_count;
		child_segment->count += copy_count;
	}
	child_segments.total_capacity += str_size;
	// store the updated linked list
	Store<LinkedList>(child_segments, data_ptr_cast(GetListChildData(segment)));
}

static void WriteDataToListSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                   ListSegment *segment, RecursiveUnifiedVectorFormat &input_data, idx_t &entry_idx) {
	auto sel_entry_idx = input_data.unified.sel->get_index(entry_idx);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto valid = input_data.unified.validity.RowIsValid(sel_entry_idx);
	null_mask[segment->count] = !valid;

	// set the length of this list
	auto list_length_data = GetListLengthData(segment);
	uint64_t list_length = 0;

	if (valid) {
		// get list entry information
		const auto &list_entry = UnifiedVectorFormat::GetData<list_entry_t>(input_data.unified)[sel_entry_idx];
		list_length = list_entry.length;

		// loop over the child vector entries and recurse on them
		auto child_segments = Load<LinkedList>(data_ptr_cast(GetListChildData(segment)));
		D_ASSERT(functions.child_functions.size() == 1);
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
			auto source_idx_child = list_entry.offset + child_idx;
			functions.child_functions[0].AppendRow(allocator, child_segments, input_data.children.back(),
			                                       source_idx_child);
		}
		// store the updated linked list
		Store<LinkedList>(child_segments, data_ptr_cast(GetListChildData(segment)));
	}

	Store<uint64_t>(list_length, data_ptr_cast(list_length_data + segment->count));
}

static void WriteDataToStructSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                     ListSegment *segment, RecursiveUnifiedVectorFormat &input_data, idx_t &entry_idx) {
	auto sel_entry_idx = input_data.unified.sel->get_index(entry_idx);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto valid = input_data.unified.validity.RowIsValid(sel_entry_idx);
	null_mask[segment->count] = !valid;

	// write value
	D_ASSERT(input_data.children.size() == functions.child_functions.size());
	auto child_list = GetStructData(segment);

	// write the data of each of the children of the struct
	for (idx_t i = 0; i < input_data.children.size(); i++) {
		auto child_list_segment = Load<ListSegment *>(data_ptr_cast(child_list + i));
		auto &child_function = functions.child_functions[i];
		child_function.write_data(child_function, allocator, child_list_segment, input_data.children[i], entry_idx);
		child_list_segment->count++;
	}
}

static void WriteDataToArraySegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                    ListSegment *segment, RecursiveUnifiedVectorFormat &input_data, idx_t &entry_idx) {
	auto sel_entry_idx = input_data.unified.sel->get_index(entry_idx);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto valid = input_data.unified.validity.RowIsValid(sel_entry_idx);
	null_mask[segment->count] = !valid;

	// Arrays require there to be values in the child even when the entry is NULL.
	auto array_size = ArrayType::GetSize(input_data.logical_type);
	auto array_offset = sel_entry_idx * array_size;

	auto child_segments = Load<LinkedList>(data_ptr_cast(GetArrayChildData(segment)));
	D_ASSERT(functions.child_functions.size() == 1);
	for (idx_t elem_idx = array_offset; elem_idx < array_offset + array_size; elem_idx++) {
		functions.child_functions[0].AppendRow(allocator, child_segments, input_data.children.back(), elem_idx);
	}
	// store the updated linked list
	Store<LinkedList>(child_segments, data_ptr_cast(GetArrayChildData(segment)));
}

void ListSegmentFunctions::AppendRow(ArenaAllocator &allocator, LinkedList &linked_list,
                                     RecursiveUnifiedVectorFormat &input_data, idx_t &entry_idx) const {
	auto &write_data_to_segment = *this;
	auto segment = GetSegment(write_data_to_segment, allocator, linked_list);
	write_data_to_segment.write_data(write_data_to_segment, allocator, segment, input_data, entry_idx);

	linked_list.total_capacity++;
	segment->count++;
}

void ListSegmentFunctions::AppendListEntry(ArenaAllocator &allocator, LinkedList &linked_list,
                                           RecursiveUnifiedVectorFormat &child_data,
                                           const list_entry_t &list_entry) const {
	for (idx_t child_idx = list_entry.offset; child_idx < list_entry.offset + list_entry.length; child_idx++) {
		allocator.AlignNext();
		AppendRow(allocator, linked_list, child_data, child_idx);
	}
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
//! Move the scan state to the next segment of the linked list (clearing any child scan states)
static void MoveToNextSegment(ListSegmentScanState &state) {
	state.segment = state.segment->next;
	state.offset = 0;
	state.children.clear();
}

template <class T>
static idx_t ScanPrimitiveData(const ListSegmentFunctions &, ListSegmentScanState &state, idx_t count, Vector &result,
                               idx_t result_offset) {
	auto &aggr_vector_validity = FlatVector::ValidityMutable(result);
	auto aggr_vector_data = FlatVector::GetDataMutable<T>(result);

	idx_t total_read = 0;
	while (total_read < count && state.segment) {
		auto segment = state.segment;
		auto read_count = MinValue<idx_t>(count - total_read, segment->count - state.offset);
		auto null_mask = GetNullMask(segment);
		auto data = GetPrimitiveData<T>(segment);
		for (idx_t i = 0; i < read_count; i++) {
			auto source_idx = state.offset + i;
			auto target_idx = result_offset + total_read + i;
			if (null_mask[source_idx]) {
				aggr_vector_validity.SetInvalid(target_idx);
			} else {
				aggr_vector_data[target_idx] = Load<T>(const_data_ptr_cast(data + source_idx));
			}
		}
		total_read += read_count;
		state.offset += read_count;
		if (state.offset == segment->count) {
			MoveToNextSegment(state);
		}
	}
	return total_read;
}

static idx_t ScanVarcharData(const ListSegmentFunctions &, ListSegmentScanState &state, idx_t count, Vector &result,
                             idx_t result_offset) {
	auto &aggr_vector_validity = FlatVector::ValidityMutable(result);
	auto aggr_vector_data = FlatVector::GetDataMutable<string_t>(result);

	idx_t total_read = 0;
	while (total_read < count && state.segment) {
		auto segment = state.segment;
		if (state.children.empty()) {
			// entering this segment - point the child state at the start of the segment's character data
			auto linked_child_list = Load<LinkedList>(const_data_ptr_cast(GetListChildData(segment)));
			state.children.emplace_back();
			state.children.back().segment = linked_child_list.first_segment;
		}
		auto &child_state = state.children.back();
		auto read_count = MinValue<idx_t>(count - total_read, segment->count - state.offset);
		auto null_mask = GetNullMask(segment);
		auto str_length_data = GetListLengthData(segment);
		for (idx_t i = 0; i < read_count; i++) {
			auto source_idx = state.offset + i;
			auto target_idx = result_offset + total_read + i;
			if (null_mask[source_idx]) {
				// set to null
				aggr_vector_validity.SetInvalid(target_idx);
				continue;
			}
			// read the string
			auto &result_str = aggr_vector_data[target_idx];
			auto str_length = Load<uint64_t>(const_data_ptr_cast(str_length_data + source_idx));
			if (child_state.segment && child_state.offset + str_length <= child_state.segment->capacity) {
				// the string fits in the current segment - reference it directly instead of copying
				result_str = string_t(GetStringData(child_state.segment) + child_state.offset,
				                      NumericCast<uint32_t>(str_length));
				child_state.offset += str_length;
				if (child_state.offset == child_state.segment->capacity) {
					child_state.segment = child_state.segment->next;
					child_state.offset = 0;
				}
				continue;
			}
			// allocate an empty string for the given size
			result_str = StringVector::EmptyString(result, str_length);
			auto result_data = result_str.GetDataWriteable();
			idx_t current_offset = 0;
			while (current_offset < str_length) {
				if (!child_state.segment) {
					throw InternalException("Insufficient data to read string");
				}
				auto child_data = GetStringData(child_state.segment);
				idx_t max_copy =
				    MinValue<idx_t>(str_length - current_offset, child_state.segment->capacity - child_state.offset);
				memcpy(result_data + current_offset, child_data + child_state.offset, max_copy);
				current_offset += max_copy;
				child_state.offset += max_copy;
				if (child_state.offset >= child_state.segment->capacity) {
					D_ASSERT(child_state.offset == child_state.segment->capacity);
					child_state.segment = child_state.segment->next;
					child_state.offset = 0;
				}
			}

			// finalize the str
			result_str.Finalize();
		}
		total_read += read_count;
		state.offset += read_count;
		if (state.offset == segment->count) {
			MoveToNextSegment(state);
		}
	}
	return total_read;
}

static idx_t ScanListData(const ListSegmentFunctions &functions, ListSegmentScanState &state, idx_t count,
                          Vector &result, idx_t result_offset) {
	auto &aggr_vector_validity = FlatVector::ValidityMutable(result);
	auto list_vector_data = FlatVector::GetDataMutable<list_entry_t>(result);

	D_ASSERT(functions.child_functions.size() == 1);
	auto &child_function = functions.child_functions[0];

	idx_t total_read = 0;
	while (total_read < count && state.segment) {
		auto segment = state.segment;
		if (state.children.empty()) {
			// entering this segment - point the child state at the start of the segment's child values
			auto linked_child_list = Load<LinkedList>(const_data_ptr_cast(GetListChildData(segment)));
			state.children.emplace_back();
			state.children.back().segment = linked_child_list.first_segment;
		}
		auto &child_state = state.children.back();
		auto read_count = MinValue<idx_t>(count - total_read, segment->count - state.offset);
		auto null_mask = GetNullMask(segment);
		auto list_length_data = GetListLengthData(segment);

		// get the starting offset in the result child vector
		auto current_result_offset = result_offset + total_read;
		idx_t offset = 0;
		if (current_result_offset != 0) {
			offset =
			    list_vector_data[current_result_offset - 1].offset + list_vector_data[current_result_offset - 1].length;
		}
		idx_t starting_offset = offset;

		// set length and offsets
		for (idx_t i = 0; i < read_count; i++) {
			auto source_idx = state.offset + i;
			auto target_idx = current_result_offset + i;
			if (null_mask[source_idx]) {
				aggr_vector_validity.SetInvalid(target_idx);
			}
			auto list_length = Load<uint64_t>(const_data_ptr_cast(list_length_data + source_idx));
			list_vector_data[target_idx].length = list_length;
			list_vector_data[target_idx].offset = offset;
			offset += list_length;
		}

		ListVector::Reserve(result, offset);
		auto &child_vector = ListVector::GetChildMutable(result);

		// recurse into the linked list of child values
		auto child_read = child_function.scan_data(child_function, child_state, offset - starting_offset, child_vector,
		                                           starting_offset);
		if (child_read != offset - starting_offset) {
			throw InternalException("Insufficient data in linked list to read list entries");
		}
		ListVector::SetListSize(result, offset);

		total_read += read_count;
		state.offset += read_count;
		if (state.offset == segment->count) {
			MoveToNextSegment(state);
		}
	}
	return total_read;
}

static idx_t ScanStructData(const ListSegmentFunctions &functions, ListSegmentScanState &state, idx_t count,
                            Vector &result, idx_t result_offset) {
	auto &aggr_vector_validity = FlatVector::ValidityMutable(result);
	auto &children = StructVector::GetEntries(result);
	D_ASSERT(children.size() == functions.child_functions.size());

	idx_t total_read = 0;
	while (total_read < count && state.segment) {
		auto segment = state.segment;
		if (state.children.empty()) {
			// entering this segment - point the child states at the segment's child segments
			auto struct_children = GetStructData(segment);
			state.children.resize(children.size());
			for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
				state.children[child_idx].segment =
				    Load<ListSegment *>(const_data_ptr_cast(struct_children + child_idx));
			}
		}
		auto read_count = MinValue<idx_t>(count - total_read, segment->count - state.offset);
		auto null_mask = GetNullMask(segment);
		for (idx_t i = 0; i < read_count; i++) {
			if (null_mask[state.offset + i]) {
				aggr_vector_validity.SetInvalid(result_offset + total_read + i);
			}
		}

		// recurse into the child segments of each child of the struct
		// the child segments are aligned with the parent segment, so they advance in lockstep
		for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
			auto &child_function = functions.child_functions[child_idx];
			child_function.scan_data(child_function, state.children[child_idx], read_count, children[child_idx],
			                         result_offset + total_read);
		}

		total_read += read_count;
		state.offset += read_count;
		if (state.offset == segment->count) {
			MoveToNextSegment(state);
		}
	}
	return total_read;
}

static idx_t ScanArrayData(const ListSegmentFunctions &functions, ListSegmentScanState &state, idx_t count,
                           Vector &result, idx_t result_offset) {
	auto &aggr_vector_validity = FlatVector::ValidityMutable(result);
	auto &child_vector = ArrayVector::GetChildMutable(result);
	auto array_size = ArrayType::GetSize(result.GetType());

	D_ASSERT(functions.child_functions.size() == 1);
	auto &child_function = functions.child_functions[0];

	idx_t total_read = 0;
	while (total_read < count && state.segment) {
		auto segment = state.segment;
		if (state.children.empty()) {
			// entering this segment - point the child state at the start of the segment's child values
			auto linked_child_list = Load<LinkedList>(const_data_ptr_cast(GetArrayChildData(segment)));
			state.children.emplace_back();
			state.children.back().segment = linked_child_list.first_segment;
		}
		auto &child_state = state.children.back();
		auto read_count = MinValue<idx_t>(count - total_read, segment->count - state.offset);
		auto null_mask = GetNullMask(segment);
		for (idx_t i = 0; i < read_count; i++) {
			if (null_mask[state.offset + i]) {
				aggr_vector_validity.SetInvalid(result_offset + total_read + i);
			}
		}

		// recurse into the linked list of child values
		child_function.scan_data(child_function, child_state, read_count * array_size, child_vector,
		                         (result_offset + total_read) * array_size);

		total_read += read_count;
		state.offset += read_count;
		if (state.offset == segment->count) {
			MoveToNextSegment(state);
		}
	}
	return total_read;
}

void ListSegmentFunctions::InitializeScan(const LinkedList &linked_list, ListSegmentScanState &state) const {
	state.segment = linked_list.first_segment;
	state.offset = 0;
	state.children.clear();
}

idx_t ListSegmentFunctions::Scan(ListSegmentScanState &state, Vector &result) const {
	return scan_data(*this, state, STANDARD_VECTOR_SIZE, result, 0);
}

void ListSegmentFunctions::BuildListVector(const LinkedList &linked_list, Vector &result, idx_t total_count) const {
	ListSegmentScanState state;
	InitializeScan(linked_list, state);
	scan_data(*this, state, NumericLimits<idx_t>::Maximum(), result, total_count);
}

void ListSegmentFunctions::BuildLists(const vector<LinkedList> &linked_lists, Vector &result, idx_t offset) const {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	const idx_t count = linked_lists.size();

	auto &mask = FlatVector::ValidityMutable(result);
	auto result_data = FlatVector::ScatterWriter<list_entry_t>(result);
	idx_t total_len = ListVector::GetListSize(result);

	// first iterate over all entries and set up the list entries, and get the newly required total length
	for (idx_t i = 0; i < count; i++) {
		auto &linked_list = linked_lists[i];
		const auto rid = offset + i;
		result_data[rid].offset = total_len;
		if (linked_list.total_capacity == 0) {
			// empty linked list - set the result row to NULL
			mask.SetInvalid(rid);
			result_data[rid].length = 0;
			continue;
		}
		result_data[rid].length = linked_list.total_capacity;
		total_len += linked_list.total_capacity;
	}

	// reserve capacity, then iterate over all entries again and copy over the data to the child vector
	ListVector::Reserve(result, total_len);
	auto &result_child = ListVector::GetChildMutable(result);
	for (idx_t i = 0; i < count; i++) {
		auto &linked_list = linked_lists[i];
		if (linked_list.total_capacity == 0) {
			continue;
		}
		BuildListVector(linked_list, result_child, result_data[offset + i].offset);
	}

	ListVector::SetListSize(result, total_len);
	FlatVector::SetSize(result, offset + count);
}

//===--------------------------------------------------------------------===//
// Functions
//===--------------------------------------------------------------------===//
template <class T>
void SegmentPrimitiveFunction(ListSegmentFunctions &functions) {
	functions.create_segment = CreatePrimitiveSegment<T>;
	functions.write_data = WriteDataToPrimitiveSegment<T>;
	functions.scan_data = ScanPrimitiveData<T>;
}

void GetSegmentDataFunctions(ListSegmentFunctions &functions, const LogicalType &type) {
	if (type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	auto physical_type = type.InternalType();
	switch (physical_type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL:
		SegmentPrimitiveFunction<bool>(functions);
		functions.initial_capacity = 8;
		break;
	case PhysicalType::INT8:
		SegmentPrimitiveFunction<int8_t>(functions);
		functions.initial_capacity = 8;
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
		functions.initial_capacity = 8;
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
	case PhysicalType::UINT128:
		SegmentPrimitiveFunction<uhugeint_t>(functions);
		break;
	case PhysicalType::INTERVAL:
		SegmentPrimitiveFunction<interval_t>(functions);
		break;
	case PhysicalType::VARCHAR: {
		functions.create_segment = CreateListSegment;
		functions.write_data = WriteDataToVarcharSegment;
		functions.scan_data = ScanVarcharData;

		ListSegmentFunctions child_function;
		child_function.create_segment = CreateVarcharDataSegment;
		child_function.write_data = nullptr;
		child_function.scan_data = nullptr;
		child_function.initial_capacity = 16;
		functions.child_functions.push_back(child_function);
		break;
	}
	case PhysicalType::LIST: {
		functions.create_segment = CreateListSegment;
		functions.write_data = WriteDataToListSegment;
		functions.scan_data = ScanListData;

		// recurse
		functions.child_functions.emplace_back();
		GetSegmentDataFunctions(functions.child_functions.back(), ListType::GetChildType(type));
		break;
	}
	case PhysicalType::STRUCT: {
		functions.create_segment = CreateStructSegment;
		functions.write_data = WriteDataToStructSegment;
		functions.scan_data = ScanStructData;

		// recurse
		auto child_types = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < child_types.size(); i++) {
			functions.child_functions.emplace_back();
			GetSegmentDataFunctions(functions.child_functions.back(), child_types[i].second);
		}
		break;
	}
	case PhysicalType::ARRAY: {
		functions.create_segment = CreateArraySegment;
		functions.write_data = WriteDataToArraySegment;
		functions.scan_data = ScanArrayData;

		// recurse
		functions.child_functions.emplace_back();
		GetSegmentDataFunctions(functions.child_functions.back(), ArrayType::GetChildType(type));
		break;
	}
	default:
		throw InternalException("LIST aggregate not yet implemented for " + type.ToString());
	}
}

} // namespace duckdb
