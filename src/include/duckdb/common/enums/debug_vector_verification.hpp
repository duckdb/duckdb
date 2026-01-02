//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/debug_vector_verification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class DebugVectorVerification : uint8_t {
	NONE,
	DICTIONARY_EXPRESSION,
	DICTIONARY_OPERATOR,
	CONSTANT_OPERATOR,
	SEQUENCE_OPERATOR,
	NESTED_SHUFFLE,
	VARIANT_VECTOR
};

} // namespace duckdb
