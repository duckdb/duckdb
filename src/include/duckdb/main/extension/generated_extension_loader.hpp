//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension/generated_extension_loader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/database.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

#if defined(GENERATED_EXTENSION_HEADERS) and !defined(DUCKDB_AMALGAMATION)
#include "generated_extension_headers.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

//! Looks through the CMake-generated list of extensions that are linked into DuckDB currently to try load <extension>
bool TryLoadLinkedExtension(DuckDB &db, const string &extension);
extern vector<string> linked_extensions;
extern vector<string> loaded_extension_test_paths;

} // namespace duckdb
#endif
