//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/nested_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct HistogramFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static AggregateFunction GetHistogramUnorderedMap(LogicalType &type);
};

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

struct GetSegmentDataFunction;
typedef void (*get_data_from_segment_t)(GetSegmentDataFunction &get_segment_data_function, ListSegment *segment,
                                        Vector &result, idx_t &total_count);

struct GetSegmentDataFunction {
	// raw segment function
	get_data_from_segment_t segment_function;

	// used for lists/structs to store child function pointers
	vector<GetSegmentDataFunction> child_functions;
};
struct ListFun {
	static void RegisterFunction(BuiltinFunctions &set);

	//! Allocates a ListSegment for primitive data values. It contains the header (count, capacity, next), the
	//! null_mask and the primitive values.
	template <class T>
	static data_ptr_t AllocatePrimitiveData(Allocator &allocator, vector<AllocatedData> &owning_vector,
	                                        uint16_t &capacity);
	//! Allocates a ListSegment for LIST data values. It contains the header (count, capacity, next) and the null_mask.
	//! It also contains the length of each list as well as a linked list of the child values.
	static data_ptr_t AllocateListData(Allocator &allocator, vector<AllocatedData> &owning_vector, uint16_t &capacity);
	//! Allocates a ListSegment for STRUCT data values. It contains the header (count, capacity, next) and the
	//! null_mask.
	// It also contains pointers to the ListSegments of each child vector.
	static data_ptr_t AllocateStructData(Allocator &allocator, vector<AllocatedData> &owning_vector, uint16_t &capacity,
	                                     idx_t child_count);

	//! Get a pointer to the first position of the primitive data of a ListSegment for primitve types.
	template <class T>
	static T *TemplatedGetPrimitiveData(ListSegment *segment);
	//! Get a pointer to the list length data of a ListSegment of type LIST.
	static uint64_t *GetListLengthData(ListSegment *segment);
	//! Get a pointer to the child list of a ListSegment of type LIST.
	static LinkedList *GetListChildData(ListSegment *segment);
	//! Get a pointer to the child pointers of a ListSegment of type STRUCT.
	static ListSegment **GetStructData(ListSegment *segment);

	//! Type switch to store the vector data in the segment.
	static void SetPrimitiveDataValue(ListSegment *segment, const LogicalType &type, data_ptr_t &input_data,
	                                  idx_t &row_idx);

	//! Get a pointer to the null_mask of a ListSegment.
	static bool *GetNullMask(ListSegment *segment);

	//! Get the capacity for the next segment in a linked list.
	static uint16_t GetCapacityForNewSegment(LinkedList *linked_list);

	//! Create a new ListSegment for primitive data and set the header.
	template <class T>
	static ListSegment *TemplatedCreatePrimitiveSegment(Allocator &allocator, vector<AllocatedData> &owning_vector,
	                                                    uint16_t &capacity);
	//! Type switch for allocating a new ListSegment for primitive data.
	static ListSegment *CreatePrimitiveSegment(Allocator &allocator, vector<AllocatedData> &owning_vector,
	                                           uint16_t &capacity, const LogicalType &type);
	//! Create a new ListSegment of type LIST, set the header and an empty linked list.
	static ListSegment *CreateListSegment(Allocator &allocator, vector<AllocatedData> &owning_vector,
	                                      uint16_t &capacity);
	//! Create a new ListSegment of type STRUCT, set the header and recurse into the creation of the segments for each
	//! child.
	static ListSegment *CreateStructSegment(Allocator &allocator, vector<AllocatedData> &owning_vector,
	                                        uint16_t &capacity, vector<unique_ptr<Vector>> &children);
	//! Type switch to create the specific ListSegment types depending on the input type.
	static ListSegment *CreateSegment(Allocator &allocator, vector<AllocatedData> &owning_vector, uint16_t &capacity,
	                                  Vector &input);

	//! Get the current segment of a linked list to append data to
	static ListSegment *GetSegment(Allocator &allocator, vector<AllocatedData> &owning_vector, LinkedList *linked_list,
	                               Vector &input);
	//! Get the current char segment of a linked list to append data to (this is a specialized function for VARCHARs).
	static ListSegment *GetCharSegment(Allocator &allocator, vector<AllocatedData> &owning_vector,
	                                   LinkedList *linked_list);

	//! Write the data of the current entry of the input vector to the active segment of the linked list.
	static void WriteDataToSegment(Allocator &allocator, vector<AllocatedData> &owning_vector, ListSegment *segment,
	                               Vector &input, idx_t &entry_idx, idx_t &count);
	//! Get the currently active segment of the linked list and call WriteDataToSegment on it.
	static void AppendRow(Allocator &allocator, vector<AllocatedData> &owning_vector, LinkedList *linked_list,
	                      Vector &input, idx_t &entry_idx, idx_t &count);

	//! Get all the data of a primitive segment and write it to the result vector.
	template <class T>
	static void GetDataFromPrimitiveSegment(GetSegmentDataFunction &get_segment_data_function, ListSegment *segment,
	                                        Vector &result, idx_t &total_count);
	//! Get all the data of a varchar segment and write it to the result vector.
	static void GetDataFromVarcharSegment(GetSegmentDataFunction &get_segment_data_function, ListSegment *segment,
	                                      Vector &result, idx_t &total_count);
	//! Get all the data of a LIST segment and write it to the result vector.
	static void GetDataFromListSegment(GetSegmentDataFunction &get_segment_data_function, ListSegment *segment,
	                                   Vector &result, idx_t &total_count);
	//! Get all the data of a STRUCT segment and write it to the result vector.
	static void GetDataFromStructSegment(GetSegmentDataFunction &get_segment_data_function, ListSegment *segment,
	                                     Vector &result, idx_t &total_count);
	//! Loop over a linked list and read all its segment into the result vector.
	static void BuildListVector(GetSegmentDataFunction &get_segment_data_function, LinkedList *linked_list,
	                            Vector &result, idx_t &initial_total_count);

	//! Initialize all validity masks of a vector to the value of capacity.
	static void InitializeValidities(Vector &vector, idx_t &capacity);
};

} // namespace duckdb
