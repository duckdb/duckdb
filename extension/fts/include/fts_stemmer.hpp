//===----------------------------------------------------------------------===//
//                         DuckDB
//
// fts_stemmer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

string_t Stem(string_t);

} // namespace duckdb
