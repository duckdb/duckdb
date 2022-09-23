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

void RegisterICUStrptimeFunctions(ClientContext &context);

} // namespace duckdb
