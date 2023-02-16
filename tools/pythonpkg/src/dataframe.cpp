#include "duckdb_python/dataframe.hpp"
#include "duckdb_python/pyconnection.hpp"

namespace duckdb {
bool PolarsDataFrame::IsDataFrame(const py::handle &object) {
	if (!ModuleIsLoaded<PolarsCacheItem>()) {
		return false;
	}
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return import_cache.polars().DataFrame.IsInstance(object);
}

bool PolarsDataFrame::IsLazyFrame(const py::handle &object) {
	if (!ModuleIsLoaded<PolarsCacheItem>()) {
		return false;
	}
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return import_cache.polars().LazyFrame.IsInstance(object);
}

bool DataFrame::check_(const py::handle &object) {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return import_cache.pandas().DataFrame.IsInstance(object);
}

bool PolarsDataFrame::check_(const py::handle &object) {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return import_cache.polars().DataFrame.IsInstance(object);
}

} // namespace duckdb
