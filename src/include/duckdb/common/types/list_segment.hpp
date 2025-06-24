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

// forward declarations
struct ListSegmentFunctions;
typedef ListSegment *(*create_segment_t)(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                         uint16_t capacity);
typedef void (*write_data_to_segment_t)(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                        ListSegment *segment, RecursiveUnifiedVectorFormat &input_data,
                                        idx_t &entry_idx);
typedef void (*read_data_from_segment_t)(const ListSegmentFunctions &functions, const ListSegment *segment,
                                         Vector &result, idx_t &total_count);

struct ListSegmentFunctions {
	create_segment_t create_segment;
	write_data_to_segment_t write_data;
	read_data_from_segment_t read_data;
	uint16_t initial_capacity = ListSegment::INITIAL_CAPACITY;

	vector<ListSegmentFunctions> child_functions;

	void AppendRow(ArenaAllocator &allocator, LinkedList &linked_list, RecursiveUnifiedVectorFormat &input_data,
	               idx_t &entry_idx) const;
	void BuildListVector(const LinkedList &linked_list, Vector &result, idx_t total_count) const;
};

void GetSegmentDataFunctions(ListSegmentFunctions &functions, const LogicalType &type);
} // namespace duckdb
