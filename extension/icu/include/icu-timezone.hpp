//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-timezone.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterICUTimeZoneFunctions(DatabaseInstance &db);

} // namespace duckdb
