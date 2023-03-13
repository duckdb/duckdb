//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix_segment.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/art_node.hpp"

namespace duckdb {

class PrefixSegment {
public:
	//! Constructor of an empty prefix segment containing bytes.
	//! NOTE: only use this constructor for temporary prefix segments
	PrefixSegment();

	//! The prefix bytes stored in this segment
	uint8_t bytes[ARTNode::PREFIX_SEGMENT_SIZE];
	//! The position of the next segment, if the prefix exceeds this segment
	idx_t next;

	//! Get a new pointer to a prefix segment, might cause a new buffer allocation
	static inline void New(ART &art, idx_t &new_position) {
		art.GetAllocator(ARTNodeType::PREFIX_SEGMENT)->New(new_position);
	}
	static inline idx_t New(ART &art) {
		return art.GetAllocator(ARTNodeType::PREFIX_SEGMENT)->New();
	}
	//! Free a prefix segment
	static inline void Free(ART &art, const idx_t &position) {
		art.GetAllocator(ARTNodeType::PREFIX_SEGMENT)->Free(position);
		art.DecreaseMemorySize(sizeof(PrefixSegment));
	}
	//! Initialize all the fields of the segment
	static PrefixSegment *Initialize(ART &art, const idx_t &position);
	//! Get a prefix segment
	static inline PrefixSegment *Get(ART &art, const idx_t &position) {
		return art.GetAllocator(ARTNodeType::PREFIX_SEGMENT)->Get<PrefixSegment>(position);
	}

	//! Append a byte to the current segment, or create a new segment containing that byte
	PrefixSegment *Append(ART &art, uint32_t &count, const uint8_t &byte);
	//! Get the tail of a list of segments
	PrefixSegment *GetTail(ART &art);
};

} // namespace duckdb
