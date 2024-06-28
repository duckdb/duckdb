//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-dateadd.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterICUDateAddFunctions(DatabaseInstance &db);

} // namespace duckdb
