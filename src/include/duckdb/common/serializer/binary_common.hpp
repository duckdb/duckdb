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

} // namespace duckdb
