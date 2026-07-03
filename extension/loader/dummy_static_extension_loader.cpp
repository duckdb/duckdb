#include "duckdb/main/extension_helper.hpp"

// This is a dummy loader to produce a workable duckdb library without linking any extensions.
// Link this to libduckdb_static.a to get a working system.

namespace duckdb {
ExtensionLoadResult ExtensionHelper::LoadExtension(DuckDB &db, const string &extension) {
	return ExtensionLoadResult::NOT_LOADED;
}

void ExtensionHelper::LoadAllExtensions(DuckDB &db) {
	// nop
}

vector<string> ExtensionHelper::LoadedExtensionTestPaths() {
	return {};
}

} // namespace duckdb
