//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/swizzleable_pointer.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

// classes
class MetaBlockReader;

// structs
struct BlockPointer;

//! SwizzleablePointer provides functions on a (possibly) swizzled pointer. If the swizzle flag is set, then the
//! pointer points to a storage address (and has no type), otherwise the pointer has a type and stores
//! other information (e.g., a buffer location)
class SwizzleablePointer {
public:
	//! Constructs an empty SwizzleablePointer
	SwizzleablePointer() : swizzle_flag(0), type(0), offset(0), buffer_id(0) {};
	//! Constructs a swizzled pointer from a buffer ID and an offset
	explicit SwizzleablePointer(MetaBlockReader &reader);
	//! Constructs a non-swizzled pointer from a buffer ID and an offset
	SwizzleablePointer(uint32_t offset, uint32_t buffer_id)
	    : swizzle_flag(0), type(0), offset(offset), buffer_id(buffer_id) {};

	//! The swizzle flag, set if swizzled, not set otherwise
	uint8_t swizzle_flag : 1;
	//! The type of the pointer, zero if not set
	uint8_t type : 7;
	//! The offset of a memory location
	uint32_t offset : 24;
	//! The buffer ID of a memory location
	uint32_t buffer_id : 32;

public:
	//! Checks if the pointer is swizzled
	inline bool IsSwizzled() const {
		return swizzle_flag;
	}
	//! Returns true, if neither the swizzle flag nor the type is set, and false otherwise
	inline bool IsSet() const {
		return swizzle_flag || type;
	}
	//! Reset the pointer
	inline void Reset() {
		swizzle_flag = 0;
		type = 0;
	}
};

} // namespace duckdb
