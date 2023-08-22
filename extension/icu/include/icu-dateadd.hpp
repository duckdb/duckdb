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

void RegisterICUDateAddFunctions(DatabaseInstance &instance);

} // namespace duckdb
