//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-datepart.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterICUDatePartFunctions(ClientContext &context);

} // namespace duckdb
