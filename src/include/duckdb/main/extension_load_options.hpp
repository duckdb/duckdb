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
	ExtensionLoadOptions(string extension_name_or_path) : extension_name_or_path(std::move(extension_name_or_path)) {
	}

	//! Either a logical extension name or a full path to an extension binary - use ExtensionHelper::IsFullPath to
	//! tell them apart
	string extension_name_or_path;
	Identifier alias;
};

} // namespace duckdb
