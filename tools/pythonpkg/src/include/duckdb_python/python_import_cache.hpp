//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/python_object_container.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb.hpp"

#include "datetime.h" //From python

namespace duckdb {

//Every cache item knows how to retrieve its object
struct PythonImportCacheItem {
public:
	//Modules dont have a source
	PythonImportCacheItem() : source(*this), object(nullptr) {}

	PythonImportCacheItem(PythonImportCacheItem& source) : source(source), object(nullptr) {}
	virtual ~PythonImportCacheItem() {}
	py::handle operator () (void) {
		if (!object) {
			object = LoadObject();
		}
		return object;
	}
protected:
	//! Implemented by the class that derives from this
	virtual PyObject* LoadObject() = 0;
protected:
	//! Where to get the item from
	PythonImportCacheItem& source;
	//! The stored item
	PyObject* object;
};

//! --------------- NumPy ---------------

struct NdArrayCacheItem : public PythonImportCacheItem {
	NdArrayCacheItem(PythonImportCacheItem& source) : PythonImportCacheItem(source) {}
	PyObject* LoadObject() override {
		py::handle object_source = source();
		return object_source.attr("ndarray").ptr();
	}
};

struct NumpyCacheItem : public PythonImportCacheItem {
	NumpyCacheItem() : PythonImportCacheItem(),
		ndarray(*this)
	{}
	PyObject* LoadObject() override {
		return py::module::import("numpy").ptr();
	}
	NdArrayCacheItem ndarray;
};

//! --------------- Datetime ---------------

struct DatetimeCacheItem : public PythonImportCacheItem {
	DatetimeCacheItem(PythonImportCacheItem& source) : PythonImportCacheItem(source) {}
	PyObject* LoadObject() override {
		py::handle object_source = source();
		return object_source.attr("datetime").ptr();
	}
};

struct DateCacheItem : public PythonImportCacheItem {
	DateCacheItem(PythonImportCacheItem& source) : PythonImportCacheItem(source) {}
	PyObject* LoadObject() override {
		py::handle object_source = source();
		return object_source.attr("date").ptr();
	}
};

struct TimeCacheItem : public PythonImportCacheItem {
	TimeCacheItem(PythonImportCacheItem& source) : PythonImportCacheItem(source) {}
	PyObject* LoadObject() override {
		py::handle object_source = source();
		return object_source.attr("time").ptr();
	}
};

struct DatetimeModuleCacheItem : public PythonImportCacheItem {
	DatetimeModuleCacheItem() : PythonImportCacheItem(),
		datetime(*this),
		date(*this),
		time(*this)
	{}
	PyObject* LoadObject() override {
		return py::module::import("datetime").ptr();
	}
	DatetimeCacheItem datetime;
	DateCacheItem date;
	TimeCacheItem time;
};

struct PythonImportCache {
	NumpyCacheItem numpy;
	DatetimeModuleCacheItem datetime;
};

} //namespace duckdb
