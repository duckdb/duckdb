//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/strnstrn.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
char *strnstrn(const char *s, const char *find, idx_t s_len, idx_t find_len);

} // namespace duckdb
