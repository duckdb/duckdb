//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_load_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {

struct ExtensionLoadOptions {
	string extension_name;
	Identifier alias;
};

} // namespace duckdb
