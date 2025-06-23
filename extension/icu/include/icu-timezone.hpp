//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-timezone.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterICUTimeZoneFunctions(ExtensionLoader &loader);

} // namespace duckdb
