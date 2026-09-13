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
	//! The repository named in the FROM clause the extension is loaded from (e.g. LOAD httpfs FROM core). Empty for a
	//! bare LOAD, which only resolves core and community extensions
	string repository;
	//! Set for an autoload: only core extensions are autoloadable, so only the core keys are trusted
	bool core_only = false;
};

} // namespace duckdb
