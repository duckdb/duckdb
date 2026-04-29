//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_load_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

struct ExtensionLoadOptions {
	string extension_name;
	string alias = "";
	bool suffix_functions_with_alias = false;
};

} // namespace duckdb
