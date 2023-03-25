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
struct CopyDataFromSegment;
typedef ListSegment *(*create_segment_t)(WriteDataToSegment &write_data_to_segment, Allocator &allocator,
                                         vector<AllocatedData> &owning_vector, const uint16_t &capacity);
typedef void (*write_data_to_segment_t)(WriteDataToSegment &write_data_to_segment, Allocator &allocator,
                                        vector<AllocatedData> &owning_vector, ListSegment *segment, Vector &input,
                                        idx_t &entry_idx, idx_t &count);
typedef void (*read_data_from_segment_t)(ReadDataFromSegment &read_data_from_segment, const ListSegment *segment,
                                         Vector &result, idx_t &total_count);
typedef ListSegment *(*copy_data_from_segment_t)(CopyDataFromSegment &copy_data_from_segment, const ListSegment *source,
                                                 Allocator &allocator, vector<AllocatedData> &owning_vector);

struct WriteDataToSegment {
	create_segment_t create_segment;
	write_data_to_segment_t segment_function;
	vector<WriteDataToSegment> child_functions;
	void AppendRow(Allocator &allocator, vector<AllocatedData> &owning_vector, LinkedList *linked_list, Vector &input,
	               idx_t &entry_idx, idx_t &count);
};

struct ReadDataFromSegment {
	read_data_from_segment_t segment_function;
	vector<ReadDataFromSegment> child_functions;
	void BuildListVector(LinkedList *linked_list, Vector &result, idx_t &initial_total_count);
};

struct CopyDataFromSegment {
	copy_data_from_segment_t segment_function;
	vector<CopyDataFromSegment> child_functions;
	void CopyLinkedList(const LinkedList *source_list, LinkedList &target_list, Allocator &allocator,
	                    vector<AllocatedData> &owning_vector);
};

void GetSegmentDataFunctions(WriteDataToSegment &write_data_to_segment, ReadDataFromSegment &read_data_from_segment,
                             CopyDataFromSegment &copy_data_from_segment, const LogicalType &type);
} // namespace duckdb
