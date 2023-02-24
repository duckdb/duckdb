//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/art_node.hpp"

namespace duckdb {

// classes
class ART;

class Prefix {
public:
	//! Inlined empty prefix
	Prefix();
	//! Inlined prefix containing one byte
	explicit Prefix(const uint8_t &byte);

	//! Delete copy/assign operator
	Prefix(const Prefix &) = delete;
	//! Delete move operator
	void operator=(const Prefix &) = delete;

	//! Number of bytes in this prefix
	uint32_t count;
	union {
		//! Position to the head of the list of prefix segments
		idx_t position;
		//! Inlined prefix bytes
		uint8_t inlined[ARTNode::PREFIX_INLINE_BYTES];
	} data;

public:
	//! Initializes all the fields of the prefix
	void Initialize();

	//! Get the data at pos
	uint8_t GetData(ART &art, const idx_t &pos);
	//! Move a prefix into this prefix. NOTE: both prefixes must be in the same ART
	void Move(Prefix &other);
	//! Append a prefix to this prefix
	void Append(ART &art, ART &other_art, Prefix &other);
	//! Concatenate prefix with a partial key byte and another prefix: other.prefix + byte + this->prefix
	void Concatenate(ART &art, const uint8_t &byte, ART &other_art, Prefix &other);

	//! Deserialize this prefix
	void Deserialize(ART &art, MetaBlockReader &reader);

	//! Returns the in-memory size
	idx_t MemorySize();

private:
	//! Returns whether this prefix is inlined
	bool IsInlined() const;
	//! Moves all inlined bytes onto a prefix segment, does not change the size
	//! so this will be an (temporarily) invalid prefix
	void MoveInlinedToSegment(ART &art);
};

} // namespace duckdb
