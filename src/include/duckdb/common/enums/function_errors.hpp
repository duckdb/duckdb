//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/function_errors.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! Whether or not a function can throw an error or not
enum class FunctionErrors : uint8_t { CANNOT_ERROR = 0, CAN_THROW_RUNTIME_ERROR = 1 };

} // namespace duckdb
