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
	//! Optional human-readable reason describing why this load was triggered
	//! (e.g. explicit SQL, or autoload due to a missing function). Surfaced by the
	//! ExtensionLoadInstall log type.
	string reason;
};

} // namespace duckdb
