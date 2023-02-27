//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/leaf_segment.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/art_node.hpp"

namespace duckdb {

class LeafSegment {
public:
	//! The row IDs stored in this segment
	row_t row_ids[ARTNode::LEAF_SEGMENT_SIZE];
	//! The position of the next segment, if the row IDs exceeds this segment
	idx_t next;

	//! Initialize all the fields of the segment
	static LeafSegment *Initialize(ART &art, const idx_t &position);
	//! Appends a row ID to the current segment, or creates a new segment containing that row ID
	LeafSegment *Append(ART &art, uint32_t &count, const row_t &row_id);
};

} // namespace duckdb
