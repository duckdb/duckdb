//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-list-range.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterICUListRangeFunctions(ExtensionLoader &loader);

} // namespace duckdb
