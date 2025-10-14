#include "duckdb/main/extension_helper.hpp"

// This is a dummy loader to produce a workable duckdb library without linking any extensions.
// Link this to libduckdb_static.a to get a working system.

namespace duckdb {
void ExtensionHelper::LoadAllExtensions(DuckDB &db) {
	// nop
}
} // namespace duckdb
