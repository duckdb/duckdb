//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

enum class BufferType : uint8_t {
	FILE_BUFFER = 1,
	MANAGED_BUFFER = 2
};

class Buffer {
public:
	Buffer(BufferType type) : type(type) {}
	virtual ~Buffer() {}

	//! The type of the buffer
	BufferType type;
};

}
