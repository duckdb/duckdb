//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/ART_data_segment.hpp
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

	//! The prefix bytes stored in this section
	uint8_t bytes[ARTNode::PREFIX_SECTION_SIZE];
	//! The position of the next section, if the prefix exceeds this section
	idx_t next;

	//! Initialize all the fields of the prefix section
	static PrefixSegment *Initialize(ART &art, const idx_t &position);
	//! Get the tail of the list of prefix sections
	static PrefixSegment *GetTail(ART &art, idx_t position);
	//! Appends a byte to the current section, or creates a new section containing that byte
	void AppendByte(ART &art, idx_t &count, const uint8_t &byte);
	//! Get the memory size
	idx_t MemorySize();
};

} // namespace duckdb
