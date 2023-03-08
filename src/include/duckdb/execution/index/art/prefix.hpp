//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/art_key.hpp"

namespace duckdb {

// classes
class ART;
class PrefixSegment;

class Prefix {
public:
	//! Inlined empty prefix
	Prefix(); // TODO: necessary?
	//! Inlined prefix containing one byte
	explicit Prefix(const uint8_t &byte); // TODO: necessary?

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
	//! Delete all prefix segments (if not inlined) and reset all fields
	void Free(ART &art);
	//! Initializes all the fields of an empty prefix
	void Initialize();
	//! Initialize a prefix from an ART key
	void Initialize(ART &art, const Key &key, const uint32_t &depth, const uint32_t &count_p);
	//! Initialize a prefix from another prefix up to count
	void Initialize(ART &art, const Prefix &other, const uint32_t &count_p);

	//! Vacuum the prefix
	void Vacuum(ART &art);

	//! Initializes a merge by incrementing the buffer IDs of the prefix segments
	void InitializeMerge(ART &art, const idx_t &buffer_count);

	//! Move a prefix into this prefix. NOTE: both prefixes must be in the same ART
	void Move(Prefix &other);
	//! Append a prefix to this prefix
	void Append(ART &art, Prefix &other);
	//! Concatenate prefix with a partial key byte and another prefix: other.prefix + byte + this->prefix
	void Concatenate(ART &art, const uint8_t &byte, Prefix &other);
	//! Removes the first n bytes, and returns the new first byte
	uint8_t Reduce(ART &art, const idx_t &n);

	//! Get the byte at position
	uint8_t GetByte(ART &art, const idx_t &position) const;
	//! Compare the key with the prefix of the node, return the position where they mismatch
	uint32_t KeyMismatchPosition(ART &art, const Key &key, const uint32_t &depth) const;
	//! Compare this prefix to another prefix, return the position where they mismatch, or count otherwise
	uint32_t MismatchPosition(ART &art, const Prefix &other) const;

	//! Serialize this prefix
	void Serialize(ART &art, MetaBlockWriter &writer);
	//! Deserialize this prefix
	void Deserialize(ART &art, MetaBlockReader &reader);

private:
	//! Returns whether this prefix is inlined
	bool IsInlined() const;
	//! Moves all inlined bytes onto a prefix segment, does not change the size
	//! so this will be an (temporarily) invalid prefix
	void MoveInlinedToSegment(ART &art);
	//! Inlines up to eight bytes on the first prefix segment
	void MoveSegmentToInlined(ART &art);
};

} // namespace duckdb
