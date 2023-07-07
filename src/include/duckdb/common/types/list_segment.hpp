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
	constexpr const static idx_t INITIAL_CAPACITY = 4;

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
struct ListSegmentFunctions;
typedef ListSegment *(*create_segment_t)(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                         uint16_t capacity);
typedef void (*write_data_to_segment_t)(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                        ListSegment *segment, Vector &input, idx_t &entry_idx, idx_t &count);
typedef void (*read_data_from_segment_t)(const ListSegmentFunctions &functions, const ListSegment *segment,
                                         Vector &result, idx_t &total_count);
typedef ListSegment *(*copy_data_from_segment_t)(const ListSegmentFunctions &functions, const ListSegment *source,
                                                 ArenaAllocator &allocator);

struct ListSegmentFunctions {
	create_segment_t create_segment;
	write_data_to_segment_t write_data;
	read_data_from_segment_t read_data;
	copy_data_from_segment_t copy_data;
	vector<ListSegmentFunctions> child_functions;

	void AppendRow(ArenaAllocator &allocator, LinkedList &linked_list, Vector &input, idx_t &entry_idx,
	               idx_t &count) const;
	void BuildListVector(const LinkedList &linked_list, Vector &result, idx_t &initial_total_count) const;
	void CopyLinkedList(const LinkedList &source_list, LinkedList &target_list, ArenaAllocator &allocator) const;
};

void GetSegmentDataFunctions(ListSegmentFunctions &functions, const LogicalType &type);
} // namespace duckdb
