//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

// classes
class ARTKey;
class PrefixSegment;

class Prefix {
public:
	//! Number of bytes in this prefix
	uint32_t count;
	union {
		//! Pointer to the head of the list of prefix segments
		Node ptr;
		//! Inlined prefix bytes
		uint8_t inlined[Node::PREFIX_INLINE_BYTES];
	} data;

public:
	//! Delete all prefix segments (if not inlined) and reset all fields
	void Free(ART &art);
	//! Initializes all the fields of an empty prefix
	inline void Initialize() {
		count = 0;
	}
	//! Initialize a prefix from an ART key
	void Initialize(ART &art, const ARTKey &key, const uint32_t depth, const uint32_t count_p);
	//! Initialize a prefix from another prefix up to count
	void Initialize(ART &art, const Prefix &other, const uint32_t count_p);

	//! Initializes a merge by incrementing the buffer IDs of the prefix segments
	void InitializeMerge(ART &art, const idx_t buffer_count);

	//! Move a prefix into this prefix
	inline void Move(Prefix &other) {
		count = other.count;
		data = other.data;
		other.Initialize();
	}
	//! Append a prefix to this prefix
	void Append(ART &art, const Prefix &other);
	//! Concatenate prefix with a partial key byte and another prefix: other.prefix + byte + this->prefix
	void Concatenate(ART &art, const uint8_t byte, const Prefix &other);
	//! Removes the first n bytes, and returns the new first byte
	uint8_t Reduce(ART &art, const idx_t reduce_count);

	//! Get the byte at position
	uint8_t GetByte(const ART &art, const idx_t position) const;
	//! Compare the key with the prefix of the node, return the position where they mismatch
	uint32_t KeyMismatchPosition(const ART &art, const ARTKey &key, const uint32_t depth) const;
	//! Compare this prefix to another prefix, return the position where they mismatch, or count otherwise
	uint32_t MismatchPosition(const ART &art, const Prefix &other) const;

	//! Serialize this prefix
	void Serialize(const ART &art, MetaBlockWriter &writer) const;
	//! Deserialize this prefix
	void Deserialize(ART &art, MetaBlockReader &reader);

	//! Vacuum the prefix segments of a prefix, if not inlined
	void Vacuum(ART &art);

private:
	//! Returns whether this prefix is inlined
	inline bool IsInlined() const {
		return count <= Node::PREFIX_INLINE_BYTES;
	}
	//! Moves all inlined bytes onto a prefix segment, does not change the size
	//! so this will be an (temporarily) invalid prefix
	PrefixSegment &MoveInlinedToSegment(ART &art);
	//! Inlines up to eight bytes on the first prefix segment
	void MoveSegmentToInlined(ART &art);
};

} // namespace duckdb
