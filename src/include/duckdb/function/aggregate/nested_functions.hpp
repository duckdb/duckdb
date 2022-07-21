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
	idx_t total_capacity = 0;
	ListSegment *first_segment = nullptr;
	ListSegment *last_segment = nullptr;
};
struct ListFun {
	static void RegisterFunction(BuiltinFunctions &set);

	// allocators
	template <class T>
	static void *AllocatePrimitiveData(uint16_t &capacity);
	static void *AllocateListData(uint16_t &capacity);
	static void *AllocateStructData(uint16_t &capacity, idx_t child_count);

	// getting data pointers
	template <class T>
	static T *TemplatedGetPrimitiveData(ListSegment *segment);
	static void GetPrimitiveDataValue(ListSegment *segment, const LogicalType &type, data_ptr_t &vector_data,
	                                  idx_t &segment_idx, idx_t row_idx);
	static list_entry_t *GetListOffsetData(ListSegment *segment);
	static LinkedList *GetListChildData(ListSegment *segment);
	static ListSegment **GetStructData(ListSegment *segment);

	// writing data
	static void SetPrimitiveDataValue(ListSegment *segment, const LogicalType &type, data_ptr_t &input_data,
	                                  idx_t &row_idx);

	// get the null mask
	static bool *GetNullMask(ListSegment *segment);

	// segment creation
	static uint16_t GetCapacityForNewSegment(LinkedList *linked_list);
	template <class T>
	static ListSegment *TemplatedCreatePrimitiveSegment(uint16_t &capacity);
	static ListSegment *CreatePrimitiveSegment(uint16_t &capacity, const LogicalType &type);
	static ListSegment *CreateListSegment(uint16_t &capacity);
	static ListSegment *CreateStructSegment(uint16_t &capacity, vector<unique_ptr<Vector>> &children);
	static ListSegment *CreateSegment(uint16_t &capacity, Vector &input);

	static ListSegment *GetSegment(LinkedList *linked_list, Vector &input);
	static ListSegment *GetCharSegment(LinkedList *linked_list);
	static void WriteDataToSegment(ListSegment *segment, Vector &input, idx_t &entry_idx, idx_t &count);
	static void AppendRow(LinkedList *linked_list, Vector &input, idx_t &entry_idx, idx_t &count);
	static void GetDataFromSegment(ListSegment *segment, Vector &result, idx_t &total_count);
	static void BuildListVector(LinkedList *linked_list, Vector &result);

	static void DestroyLinkedList(LinkedList *linked_list, const LogicalType &type);
	static void DestroySegment(ListSegment *segment, const LogicalType &type);
};

} // namespace duckdb
