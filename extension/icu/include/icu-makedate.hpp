//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-makedate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterICUMakeDateFunctions(ExtensionLoader &loader);

} // namespace duckdb
