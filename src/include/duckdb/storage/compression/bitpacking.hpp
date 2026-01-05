//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/bitpacking.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string_util.hpp"

namespace duckdb {

enum class BitpackingMode : uint8_t { INVALID, AUTO, CONSTANT, CONSTANT_DELTA, DELTA_FOR, FOR };

BitpackingMode BitpackingModeFromString(const string &str);
string BitpackingModeToString(const BitpackingMode &mode);

} // namespace duckdb
