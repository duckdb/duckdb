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

class SwizzleablePointer {
public:
	~SwizzleablePointer();
	explicit SwizzleablePointer(duckdb::MetaBlockReader &reader);
	SwizzleablePointer() : pointer(0) {};

	uint64_t pointer;

	//! Transforms from Node* to uint64_t
	SwizzleablePointer &operator=(const Node *ptr);
	friend bool operator!=(const SwizzleablePointer &s_ptr, const uint64_t &ptr);

	//! Extracts block info from swizzled pointer
	BlockPointer GetSwizzledBlockInfo();
	//! Checks if pointer is swizzled
	bool IsSwizzled();
	//! Deletes the underlying object (if necessary) and set the pointer to null_ptr
	void Reset();
	//! Unswizzle the pointer (if possible)
	Node *Unswizzle(ART &art);

	BlockPointer Serialize(ART &art, duckdb::MetaBlockWriter &writer);
};
} // namespace duckdb
