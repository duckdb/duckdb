//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/python_import_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb_python/python_object_container.hpp"
#include "duckdb_python/import_cache/python_import_cache_modules.hpp"

namespace duckdb {

struct PythonImportCache {
public:
	explicit PythonImportCache() {
		py::gil_scoped_acquire acquire;
		numpy.LoadModule("numpy", *this);
		datetime.LoadModule("datetime", *this);
		decimal.LoadModule("decimal", *this);
		uuid.LoadModule("uuid", *this);
		pandas.LoadModule("pandas", *this);
		arrow.LoadModule("pyarrow", *this);
		IPython.LoadModule("IPython", *this);
		ipywidgets.LoadModule("ipywidgets", *this);
	}
	~PythonImportCache();

public:
	NumpyCacheItem numpy;
	DatetimeCacheItem datetime;
	DecimalCacheItem decimal;
	UUIDCacheItem uuid;
	PandasCacheItem pandas;
	ArrowCacheItem arrow;
	IPythonCacheItem IPython;
	IpywidgetsCacheItem ipywidgets;

public:
	PyObject *AddCache(py::object item);

private:
	vector<py::object> owned_objects;
};

} // namespace duckdb
