//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/binary_common.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/common/typedefs.hpp"

#pragma once

namespace duckdb {

enum class BinaryMessageKind : uint8_t {
	FIXED_8 = 1,      // 1 byte
	FIXED_16 = 2,     // 2 bytes
	FIXED_32 = 3,     // 4 bytes
	FIXED_64 = 4,     // 8 bytes
	VARIABLE_LEN = 5, // 8 bytes length + data
};

// Writes a variable encoded integer to the buffer
// Returns the length of the varint (in bytes)
template <class T>
idx_t EncodeVarInt(T value, data_ptr_t target) {
	idx_t len = 0;
	do {
		uint8_t byte = value & 0x7f;
		value >>= 7;
		if (value != 0) {
			byte |= 0x80;
		}
		target[len++] = byte;
	} while (value != 0);
	return len;
}

// Reads a variable encoded integer from the buffer
// Returns the length of the varint (in bytes)
template <class T>
idx_t DecodeVarInt(data_ptr_t source, T &result) {
	idx_t len = 0;
	result = 0;
	uint8_t byte;
	do {
		byte = source[len++];
		result = (result << 7) | (byte & 0x7f);
	} while (byte >= 0x80);
	return len;
}

} // namespace duckdb
