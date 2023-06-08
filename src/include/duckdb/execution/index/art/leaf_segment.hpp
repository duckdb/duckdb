//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/leaf_segment.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

class LeafSegment {
public:
	//! The row IDs stored in this segment
	row_t row_ids[Node::LEAF_SEGMENT_SIZE];
	//! The pointer of the next segment, if the row IDs exceeds this segment
	Node next;

public:
	//! Get a new leaf segment node, might cause a new buffer allocation, and initialize it
	static LeafSegment &New(ART &art, Node &node);
	//! Get a reference to the leaf segment
	static inline LeafSegment &Get(const ART &art, const Node ptr) {
		return *Node::GetAllocator(art, NType::LEAF_SEGMENT).Get<LeafSegment>(ptr);
	}
	//! Free the leaf segment and any subsequent ones
	static void Free(ART &art, Node &node);

	//! Append a row ID to the current segment, or create a new segment containing that row ID
	LeafSegment &Append(ART &art, uint32_t &count, const row_t row_id);
	//! Get the tail of a list of segments
	LeafSegment &GetTail(const ART &art);
};

} // namespace duckdb
