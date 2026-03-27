//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shortcuts.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"

namespace duckdb {

struct ShortcutEntry {
	const char *key_name;
	const char *description;
	const char *category;
};

//! Returns the list of keyboard shortcuts (for .help shortcuts)
vector<ShortcutEntry> GetShellShortcuts();

} // namespace duckdb
