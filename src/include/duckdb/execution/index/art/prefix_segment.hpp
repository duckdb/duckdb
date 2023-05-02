//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix_segment.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

class PrefixSegment {
public:
	//! Constructor of an empty prefix segment containing bytes.
	//! NOTE: only use this constructor for temporary prefix segments
	PrefixSegment() {};

	//! The prefix bytes stored in this segment
	uint8_t bytes[Node::PREFIX_SEGMENT_SIZE];
	//! The position of the next segment, if the prefix exceeds this segment
	Node next;

public:
	//! Get a new prefix segment node, might cause a new buffer allocation, and initialize it
	static PrefixSegment &New(ART &art, Node &node);
	//! Get a reference to the prefix segment
	static inline PrefixSegment &Get(const ART &art, const Node ptr) {
		return *Node::GetAllocator(art, NType::PREFIX_SEGMENT).Get<PrefixSegment>(ptr);
	}

	//! Append a byte to the current segment, or create a new segment containing that byte
	PrefixSegment &Append(ART &art, uint32_t &count, const uint8_t byte);
	//! Get the tail of a list of segments
	PrefixSegment &GetTail(const ART &art);
};

} // namespace duckdb
