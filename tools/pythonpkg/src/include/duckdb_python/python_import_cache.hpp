//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/python_object_container.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb_python/python_object_container.hpp"

#include "datetime.h" //From python

namespace duckdb {

struct PythonImportCache;

struct PythonImportCacheItem {
public:
	PythonImportCacheItem() : object(nullptr) {
	}
	virtual ~PythonImportCacheItem() {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) {
	}

public:
	py::handle operator()(void) const;
	void LoadModule(const string &name, PythonImportCache &cache);
	void LoadAttribute(const string &name, PythonImportCache &cache, PythonImportCacheItem &source);

private:
	PyObject *AddCache(PythonImportCache &cache, py::object object);

private:
	//! The stored item
	PyObject *object;
};

//===--------------------------------------------------------------------===//
// Modules
//===--------------------------------------------------------------------===//

struct DecimalCacheItem : public PythonImportCacheItem {
public:
	~DecimalCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		Decimal.LoadAttribute("Decimal", cache, *this);
	}

public:
	//! decimal.Decimal
	PythonImportCacheItem Decimal;
};

struct UUIDCacheItem : public PythonImportCacheItem {
public:
	~UUIDCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		UUID.LoadAttribute("UUID", cache, *this);
	}

public:
	//! uuid.UUID
	PythonImportCacheItem UUID;
};

struct NumpyCacheItem : public PythonImportCacheItem {
public:
	~NumpyCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		ndarray.LoadAttribute("ndarray", cache, *this);
	}

public:
	PythonImportCacheItem ndarray;
};

struct DatetimeCacheItem : public PythonImportCacheItem {
public:
	~DatetimeCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		datetime.LoadAttribute("datetime", cache, *this);
		date.LoadAttribute("date", cache, *this);
		time.LoadAttribute("time", cache, *this);
		timedelta.LoadAttribute("timedelta", cache, *this);
	}

public:
	PythonImportCacheItem datetime;
	PythonImportCacheItem date;
	PythonImportCacheItem time;
	PythonImportCacheItem timedelta;
};

//===--------------------------------------------------------------------===//
// PythonImportCache
//===--------------------------------------------------------------------===//

struct PythonImportCache {
public:
	explicit PythonImportCache() {
		py::gil_scoped_acquire acquire;
		numpy.LoadModule("numpy", *this);
		datetime.LoadModule("datetime", *this);
		decimal.LoadModule("decimal", *this);
		uuid.LoadModule("uuid", *this);
	}
	~PythonImportCache();

public:
	NumpyCacheItem numpy;
	DatetimeCacheItem datetime;
	DecimalCacheItem decimal;
	UUIDCacheItem uuid;

public:
	PyObject *AddCache(py::object item);

private:
	vector<py::object> owned_objects;
};

} // namespace duckdb
