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
	//! Constructor of an empty prefix segment containing bytes. NOTE: only use this constructor for temporary prefix
	//! segments
	PrefixSegment();

	//! The prefix bytes stored in this segment
	uint8_t bytes[ARTNode::PREFIX_SEGMENT_SIZE];
	//! The position of the next segment, if the prefix exceeds this segment
	idx_t next;

	//! Initialize all the fields of the segment
	static PrefixSegment *Initialize(ART &art, const idx_t &position);
	//! Appends a byte to the current segment, or creates a new segment containing that byte
	PrefixSegment *Append(ART &art, uint32_t &count, const uint8_t &byte);
	//! Get the tail of a list of segments
	PrefixSegment *GetTail(ART &art, idx_t &position);
};

} // namespace duckdb
