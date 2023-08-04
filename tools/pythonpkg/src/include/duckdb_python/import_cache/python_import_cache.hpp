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
	template <class T>
	T &LazyLoadModule(T &module) {
		if (!module.LoadSucceeded()) {
			module.LoadModule(T::Name, *this);
		}
		return module;
	}

	NumpyCacheItem &numpy() {
		return LazyLoadModule(numpy_module);
	}
	TypesCacheItem &types() {
		return LazyLoadModule(types_module);
	}
	TypingCacheItem &typing() {
		return LazyLoadModule(typing_module);
	}
	PyDuckDBCacheItem &pyduckdb() {
		return LazyLoadModule(pyduckdb_module);
	}
	DatetimeCacheItem &datetime() {
		return LazyLoadModule(datetime_module);
	}
	DecimalCacheItem &decimal() {
		return LazyLoadModule(decimal_module);
	}
	PyTzCacheItem &pytz() {
		return LazyLoadModule(pytz_module);
	}
	UUIDCacheItem &uuid() {
		return LazyLoadModule(uuid_module);
	}
	PathLibCacheItem &pathlib() {
		return LazyLoadModule(pathlib_module);
	}
	PandasCacheItem &pandas() {
		return LazyLoadModule(pandas_module);
	}
	PolarsCacheItem &polars() {
		return LazyLoadModule(polars_module);
	}
	ArrowLibCacheItem &arrow_lib() {
		return LazyLoadModule(arrow_lib_module);
	}
	ArrowDatasetCacheItem &arrow_dataset() {
		return LazyLoadModule(arrow_dataset_module);
	}
	IPythonCacheItem &IPython() {
		return LazyLoadModule(IPython_module);
	}
	IpywidgetsCacheItem &ipywidgets() {
		return LazyLoadModule(ipywidgets_module);
	}

private:
	NumpyCacheItem numpy_module;
	TypesCacheItem types_module;
	TypingCacheItem typing_module;
	PathLibCacheItem pathlib_module;
	PyDuckDBCacheItem pyduckdb_module;
	DatetimeCacheItem datetime_module;
	DecimalCacheItem decimal_module;
	PyTzCacheItem pytz_module;
	UUIDCacheItem uuid_module;
	PandasCacheItem pandas_module;
	PolarsCacheItem polars_module;
	ArrowDatasetCacheItem arrow_dataset_module;
	ArrowLibCacheItem arrow_lib_module;

	IPythonCacheItem IPython_module;
	IpywidgetsCacheItem ipywidgets_module;

public:
	py::handle AddCache(py::object item);

private:
	vector<py::object> owned_objects;
};

} // namespace duckdb
