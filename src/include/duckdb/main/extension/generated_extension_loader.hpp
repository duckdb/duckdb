//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension/generated_extension_loader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifdef GENERATED_EXTENSION_HEADERS
#include "generated_extension_headers.hpp"

namespace duckdb {

//! Looks through the CMake-generated list of extensions that are linked into DuckDB currently to try load <extension>
bool TryLoadLinkedExtension(DuckDB &db, const std::string &extension);
extern vector<string> LINKED_EXTENSIONS;
extern vector<string> EXTENSION_TEST_PATHS;

} // namespace duckdb
#endif
