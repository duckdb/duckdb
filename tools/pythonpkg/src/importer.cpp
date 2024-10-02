#include "duckdb_python/import_cache/importer.hpp"
#include "duckdb_python/import_cache/python_import_cache.hpp"
#include "duckdb_python/import_cache/python_import_cache_item.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"

namespace duckdb {

py::handle PythonImporter::Import(stack<reference<PythonImportCacheItem>> &hierarchy, bool load) {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	py::handle source(nullptr);
	while (!hierarchy.empty()) {
		// From top to bottom, import them
		auto &item = hierarchy.top();
		hierarchy.pop();
		source = item.get().Load(import_cache, source, load);
		if (!source) {
			// If load is false, or the module load fails and is not required, we return early
			break;
		}
	}
	return source;
}

} // namespace duckdb
