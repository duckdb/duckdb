//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-timebucket.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterICUTimeBucketFunctions(ClientContext &context);

} // namespace duckdb
