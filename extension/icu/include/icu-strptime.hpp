//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-strptime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterICUStrptimeFunctions(DatabaseInstance &db);

} // namespace duckdb
