//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-dateadd.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterICUDateAddFunctions(ExtensionLoader &loader);

} // namespace duckdb
