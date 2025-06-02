//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-datetrunc.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterICUDateTruncFunctions(ExtensionLoader &loader);

} // namespace duckdb
