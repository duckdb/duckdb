//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension/generated_extension_loader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/database.hpp"

#if defined(GENERATED_EXTENSION_HEADERS) && !defined(DUCKDB_AMALGAMATION)
#include "duckdb/common/common.hpp"
#include "generated_extension_headers.hpp"

namespace duckdb {

//! Looks through the CMake-generated list of extensions that are linked into DuckDB currently to try load <extension>
bool TryLoadLinkedExtension(DuckDB &db, const string &extension);

const vector<string> &LinkedExtensions();
const vector<string> &LoadedExtensionTestPaths();

} // namespace duckdb
#endif
