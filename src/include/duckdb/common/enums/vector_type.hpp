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
	FSST_VECTOR,       // Contains string data compressed with FSST
	CONSTANT_VECTOR,   // Constant vector represents a single constant
	DICTIONARY_VECTOR, // Dictionary vector represents a selection vector on top of another vector
	SEQUENCE_VECTOR    // Sequence vector represents a sequence with a start point and an increment
};

string VectorTypeToString(VectorType type);

} // namespace duckdb
