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

	//! Get a new pointer to a leaf segment, might cause a new buffer allocation
	static inline void New(ART &art, idx_t &new_position) {
		art.GetAllocator(ARTNodeType::LEAF_SEGMENT)->New(new_position);
	}
	static inline idx_t New(ART &art) {
		return art.GetAllocator(ARTNodeType::LEAF_SEGMENT)->New();
	}
	//! Free a leaf segment
	static inline void Free(ART &art, const idx_t &position) {
		art.GetAllocator(ARTNodeType::LEAF_SEGMENT)->Free(position);
		art.DecreaseMemorySize(sizeof(LeafSegment));
	}
	//! Initialize all the fields of the segment
	static LeafSegment *Initialize(ART &art, const idx_t &position);
	//! Get a leaf segment
	static inline LeafSegment *Get(ART &art, const idx_t &position) {
		return art.GetAllocator(ARTNodeType::LEAF_SEGMENT)->Get<LeafSegment>(position);
	}

	//! Append a row ID to the current segment, or create a new segment containing that row ID
	LeafSegment *Append(ART &art, uint32_t &count, const row_t &row_id);
	//! Get the tail of a list of segments
	LeafSegment *GetTail(ART &art);
};

} // namespace duckdb
