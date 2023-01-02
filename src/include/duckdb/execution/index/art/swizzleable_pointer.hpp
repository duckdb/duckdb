//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/swizzleable_pointer.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

class ART;
class Node;

// SwizzleablePointer assumes that the 64-bit blockId always has 0s in the top
// 33 bits. It thus uses 8 bytes of memory rather than 12.
class SwizzleablePointer {
public:
	~SwizzleablePointer();
	explicit SwizzleablePointer(duckdb::MetaBlockReader &reader);
	SwizzleablePointer() : pointer(0) {};

	BlockPointer Serialize(ART &art, duckdb::MetaBlockWriter &writer);

	//! Transforms from Node* to uint64_t
	SwizzleablePointer &operator=(const Node *ptr);

	//! Checks if pointer is swizzled
	bool IsSwizzled();
	//! Unswizzle the pointer (if possible)
	Node *Unswizzle(ART &art);

	operator bool() const {
		return pointer;
	}

	//! Deletes the underlying object (if necessary) and set the pointer to null_ptr
	void Reset();

private:
	uint64_t pointer;

	friend bool operator!=(const SwizzleablePointer &s_ptr, const uint64_t &ptr);

	//! Extracts block info from swizzled pointer
	BlockPointer GetSwizzledBlockInfo();
};

} // namespace duckdb
