//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/list_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/vector.hpp"

#pragma once

namespace duckdb {

struct ListSegment {
	constexpr const static uint16_t INITIAL_CAPACITY = 4;

	uint16_t count;
	uint16_t capacity;
	ListSegment *next;
};
struct LinkedList {
	LinkedList() : total_capacity(0), first_segment(nullptr), last_segment(nullptr) {};
	LinkedList(idx_t total_capacity_p, ListSegment *first_segment_p, ListSegment *last_segment_p)
	    : total_capacity(total_capacity_p), first_segment(first_segment_p), last_segment(last_segment_p) {
	}

	idx_t total_capacity;
	ListSegment *first_segment;
	ListSegment *last_segment;
};

struct ListSegmentScanState {
	//! The current segment
	const ListSegment *segment = nullptr;
	//! The offset of the next row to be scanned within the current segment
	idx_t offset = 0;
	//! The scan states for the children of the current segment (if any)
	//! These are (re-)initialized by the scan whenever it moves to a new segment
	vector<ListSegmentScanState> children;
};

// forward declarations
struct ListSegmentFunctions;
typedef ListSegment *(*create_segment_t)(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                         uint16_t capacity);
typedef void (*write_data_to_segment_t)(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                        ListSegment *segment, RecursiveUnifiedVectorFormat &input_data, idx_t offset,
                                        idx_t count);
//! Scans up to "count" rows from the state's current position into the result vector at result_offset,
//! advancing the state - returns the number of rows scanned (less than "count" only if the scan is exhausted)
typedef idx_t (*scan_data_t)(const ListSegmentFunctions &functions, ListSegmentScanState &state, idx_t count,
                             Vector &result, idx_t result_offset);

struct ListSegmentFunctions {
	create_segment_t create_segment;
	write_data_to_segment_t write_data;
	scan_data_t scan_data;
	uint16_t initial_capacity = ListSegment::INITIAL_CAPACITY;

	vector<ListSegmentFunctions> child_functions;

	//! Append "count" rows of input_data (starting at "offset") to the linked list
	void AppendRows(ArenaAllocator &allocator, LinkedList &linked_list, RecursiveUnifiedVectorFormat &input_data,
	                idx_t offset, idx_t count) const;
	//! Append all rows of the given list entry (indexing into child_data) to the linked list
	void AppendListEntry(ArenaAllocator &allocator, LinkedList &linked_list, RecursiveUnifiedVectorFormat &child_data,
	                     const list_entry_t &list_entry) const;
	void BuildListVector(const LinkedList &linked_list, Vector &result, idx_t total_count) const;

	void InitializeScan(const LinkedList &linked_list, ListSegmentScanState &state) const;
	//! Scans up to STANDARD_VECTOR_SIZE rows into the (freshly initialized) result vector,
	//! returning the number of rows scanned - 0 when the scan is exhausted
	idx_t Scan(ListSegmentScanState &state, Vector &result) const;

	//! Build a LIST result vector from a set of linked lists - one per row, written at rows [offset, offset + count).
	//! Rows with an empty linked list (total_capacity == 0) are set to NULL.
	void BuildLists(const vector<LinkedList> &linked_lists, Vector &result, idx_t offset) const;
};

void GetSegmentDataFunctions(ListSegmentFunctions &functions, const LogicalType &type);

} // namespace duckdb
