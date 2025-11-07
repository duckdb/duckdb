//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-current.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterICUCurrentFunctions(ExtensionLoader &loader);

} // namespace duckdb
