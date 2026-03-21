//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/lambda_syntax.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class LambdaSyntax : uint8_t { DEFAULT = 0, ENABLE_SINGLE_ARROW = 1, DISABLE_SINGLE_ARROW = 2 };

} // namespace duckdb
