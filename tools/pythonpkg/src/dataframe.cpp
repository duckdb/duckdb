#include "duckdb_python/pybind11/dataframe.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"

namespace duckdb {
bool PolarsDataFrame::IsDataFrame(const py::handle &object) {
	if (!ModuleIsLoaded<PolarsCacheItem>()) {
		return false;
	}
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return py::isinstance(object, import_cache.polars.DataFrame());
}

bool PolarsDataFrame::IsLazyFrame(const py::handle &object) {
	if (!ModuleIsLoaded<PolarsCacheItem>()) {
		return false;
	}
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return py::isinstance(object, import_cache.polars.LazyFrame());
}

bool PandasDataFrame::check_(const py::handle &object) { // NOLINT
	if (!ModuleIsLoaded<PandasCacheItem>()) {
		return false;
	}
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return py::isinstance(object, import_cache.pandas.DataFrame());
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

	auto arrow_dtype = import_cache.pandas.ArrowDtype();
	for (auto &dtype : dtypes) {
		if (py::isinstance(dtype, arrow_dtype)) {
			return true;
		}
	}
	return false;
}

py::object PandasDataFrame::ToArrowTable(const py::object &df) {
	D_ASSERT(py::gil_check());
	try {
		return py::module_::import("pyarrow").attr("lib").attr("Table").attr("from_pandas")(df);
	} catch (py::error_already_set &) {
		// We don't fetch the original Python exception because it can cause a segfault
		// The cause of this is not known yet, for now we just side-step the issue.
		throw InvalidInputException(
		    "The dataframe could not be converted to a pyarrow.lib.Table, because a Python exception occurred.");
	}
}

bool PolarsDataFrame::check_(const py::handle &object) { // NOLINT
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return py::isinstance(object, import_cache.polars.DataFrame());
}

} // namespace duckdb
