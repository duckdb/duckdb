//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/python_import_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb_python/import_cache/python_import_cache_modules.hpp"

namespace duckdb {

struct PythonImportCache {
public:
	explicit PythonImportCache() {
	}
	~PythonImportCache();

public:
	NumpyCacheItem numpy;
	TypesCacheItem types;
	TypingCacheItem typing;
	PathLibCacheItem pathlib;
	PyDuckDBCacheItem pyduckdb;
	DatetimeCacheItem datetime;
	DecimalCacheItem decimal;
	PyTzCacheItem pytz;
	UUIDCacheItem uuid;
	PandasCacheItem pandas;
	PolarsCacheItem polars;
	ArrowLibCacheItem arrow;
	IPythonCacheItem IPython;
	IpywidgetsCacheItem ipywidgets;

public:
	py::handle AddCache(py::object item);

private:
	vector<py::object> owned_objects;
};

} // namespace duckdb
