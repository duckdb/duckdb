#include "duckdb_python/pybind11/dataframe.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"

namespace duckdb {
bool PolarsDataFrame::IsDataFrame(const py::handle &object) {
	if (!ModuleIsLoaded<PolarsCacheItem>()) {
		return false;
	}
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return py::isinstance(object, import_cache.polars().DataFrame());
}

bool PolarsDataFrame::IsLazyFrame(const py::handle &object) {
	if (!ModuleIsLoaded<PolarsCacheItem>()) {
		return false;
	}
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return py::isinstance(object, import_cache.polars().LazyFrame());
}

bool DataFrame::check_(const py::handle &object) { // NOLINT
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return py::isinstance(object, import_cache.pandas().DataFrame());
}

bool PolarsDataFrame::check_(const py::handle &object) { // NOLINT
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return py::isinstance(object, import_cache.polars().DataFrame());
}

} // namespace duckdb
