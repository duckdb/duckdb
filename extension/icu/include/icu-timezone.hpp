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

void RegisterICUTimeZoneFunctions(ClientContext &context);

} // namespace duckdb
