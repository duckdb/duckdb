//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-makedate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterICUMakeDateFunctions(DatabaseInstance &db);

} // namespace duckdb
