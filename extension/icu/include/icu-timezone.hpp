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

void RegisterICUTimeZoneFunctions(DatabaseInstance &instance);
void RegisterICUTimeZoneCasts(ClientContext &context);

} // namespace duckdb
