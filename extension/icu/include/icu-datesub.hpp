//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-datediff.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterICUDateSubFunctions(ExtensionLoader &loader);

} // namespace duckdb
