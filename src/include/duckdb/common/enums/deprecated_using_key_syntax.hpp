//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/deprecated_using_key_syntax.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class DeprecatedUsingKeySyntax : uint8_t { DEFAULT = 0, UNION_AS_UNION_ALL = 1 };

} // namespace duckdb
