//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-strptime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterICUStrptimeFunctions(ExtensionLoader &loader);

} // namespace duckdb
