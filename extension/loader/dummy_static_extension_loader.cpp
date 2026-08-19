#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_helper.hpp"

// This is a dummy loader to produce a workable duckdb library without linking any extensions.
// Link this to libduckdb_static.a to get a working system.
//
// Note that it no longer stubs out LoadExtension: that lives in core and reads the registry on the
// config, so a binary (or an extension) that links this can still load whatever it was handed.

namespace duckdb {

void ExtensionHelper::RegisterLinkedExtensions(DBConfig &config) {
	// nothing is linked into this binary
}

vector<string> ExtensionHelper::LoadedExtensionTestPaths() {
	return {};
}

} // namespace duckdb
