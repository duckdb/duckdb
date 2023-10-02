//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-datetrunc.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterICUDateTruncFunctions(DatabaseInstance &db);

} // namespace duckdb
