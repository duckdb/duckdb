//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/vector_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class VectorType : uint8_t {
	FLAT_VECTOR,       // Flat vectors represent a standard uncompressed vector
	CONSTANT_VECTOR    // Constant vector represents a single constant
};

} // namespace duckdb
