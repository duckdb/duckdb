//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/function_deserialization_cast.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! Whether or not to add casts upon deserialization
enum class FunctionDeserializationAddCast : uint8_t { DO_NOT_CAST = 0, CAST_INPUT_ARGUMENTS = 1 };

class FunctionDeserializationInfo {
public:
	FunctionDeserializationAddCast add_cast = FunctionDeserializationAddCast::DO_NOT_CAST;
};

} // namespace duckdb
