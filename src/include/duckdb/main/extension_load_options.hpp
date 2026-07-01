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
	ExtensionLoadOptions() = default;
	// NOLINTNEXTLINE: allow implicit conversion from the extension name
	ExtensionLoadOptions(string extension_name) : extension_name(std::move(extension_name)) {
	}

	string extension_name;
	Identifier alias;
};

} // namespace duckdb
