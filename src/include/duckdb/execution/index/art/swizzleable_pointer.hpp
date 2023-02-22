//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/swizzleable_pointer.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/storage/meta_block_reader.hpp"

namespace duckdb {

//! SwizzleablePointer uses the most significant bit as a flag. If the swizzle flag is set, the value in
//! the SwizzablePointer is a memory address. Otherwise, the variable stores the block information
//! of where the object is stored. In the latter case, we use the following 31 bits to store the block ID and
//! the remaining 32 bits to store the offset
class SwizzleablePointer {
public:
	//! Constructs an empty SwizzleablePointer
	SwizzleablePointer() : pointer(0) {};
	//! Constructs a swizzled pointer from a block ID and an offset
	SwizzleablePointer(MetaBlockReader &reader);

	//! Checks if the pointer is swizzled
	inline bool IsSwizzled() {
		return (pointer >> (sizeof(pointer) * 8 - 1)) & 1;
	}
	//! Get the pointer
	inline idx_t GetPointer() {
		return pointer;
	}
	//! Get the block info from a swizzled pointer
	BlockPointer GetBlockInfo();

	//! Returns true, if the pointer is set, else false
	operator bool() const {
		return pointer;
	}

protected:
	idx_t pointer;
};

} // namespace duckdb
