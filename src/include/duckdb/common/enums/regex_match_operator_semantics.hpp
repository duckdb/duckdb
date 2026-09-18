//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/regex_match_operator_semantics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class RegexMatchOperatorSemantics : uint8_t { PARTIAL = 0, FULL = 1 };

} // namespace duckdb
