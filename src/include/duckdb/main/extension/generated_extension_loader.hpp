//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension/generated_extension_loader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#if defined(GENERATED_EXTENSION_HEADERS) and !defined(DUCKDB_AMALGAMATION)
#include "generated_extension_headers.hpp"

namespace duckdb {

//! Looks through the CMake-generated list of extensions that are linked into DuckDB currently to try load <extension>
bool TryLoadLinkedExtension(DuckDB &db, const std::string &extension);
extern vector<string> linked_extensions;
extern vector<string> loaded_extension_test_paths;
extern vector<string> extensions_excluded_from_autoload;

} // namespace duckdb
#endif
