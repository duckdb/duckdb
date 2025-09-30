//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-table-range.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterICUTableRangeFunctions(ExtensionLoader &loader);

} // namespace duckdb
