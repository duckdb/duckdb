//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/leaf_segment.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"

namespace duckdb {

class LeafSegment {
public:
	//! The row IDs stored in this segment
	row_t row_ids[ARTNode::LEAF_SEGMENT_SIZE];
	//! The pointer of the next segment, if the row IDs exceeds this segment
	ARTNode next;

public:
	//! Get a new pointer to a node, might cause a new buffer allocation, and initialize it
	static LeafSegment *New(ART &art, ARTNode &node);
	//! Get a pointer to a leaf segment
	static inline LeafSegment *Get(const ART &art, const ARTNode ptr) {
		return art.leaf_segments->Get<LeafSegment>(ptr);
	}

	//! Append a row ID to the current segment, or create a new segment containing that row ID
	LeafSegment *Append(ART &art, uint32_t &count, const row_t row_id);
	//! Get the tail of a list of segments
	LeafSegment *GetTail(const ART &art);
};

} // namespace duckdb
