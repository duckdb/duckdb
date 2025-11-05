//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-timebucket.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterICUTimeBucketFunctions(ExtensionLoader &loader);

} // namespace duckdb
