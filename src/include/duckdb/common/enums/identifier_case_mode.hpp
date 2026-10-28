//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/identifier_case_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! How non-quoted identifiers are folded by the parser
enum class IdentifierCaseMode : uint8_t { PRESERVE_CASE, LOWERCASE, UPPERCASE };

} // namespace duckdb
