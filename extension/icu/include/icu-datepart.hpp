//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-datepart.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterICUDatePartFunctions(ExtensionLoader &loader);

} // namespace duckdb
