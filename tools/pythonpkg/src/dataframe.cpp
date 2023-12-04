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

bool PandasDataFrame::check_(const py::handle &object) { // NOLINT
	if (!ModuleIsLoaded<PandasCacheItem>()) {
		return false;
	}
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return py::isinstance(object, import_cache.pandas().DataFrame());
}

bool PandasDataFrame::IsPyArrowBacked(const py::handle &df) {
	if (!PandasDataFrame::check_(df)) {
		return false;
	}

	auto &import_cache = *DuckDBPyConnection::ImportCache();
	py::list dtypes = df.attr("dtypes");
	if (dtypes.empty()) {
		return false;
	}

	auto arrow_dtype = import_cache.pandas().ArrowDtype();
	for (auto &dtype : dtypes) {
		if (py::isinstance(dtype, arrow_dtype)) {
			return true;
		}
	}
	return false;
}

bool PolarsDataFrame::check_(const py::handle &object) { // NOLINT
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return py::isinstance(object, import_cache.polars().DataFrame());
}

} // namespace duckdb
