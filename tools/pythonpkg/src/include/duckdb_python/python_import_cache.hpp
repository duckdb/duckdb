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
#include "duckdb_python/registered_py_object.hpp"

#include "datetime.h" //From python

namespace duckdb {

enum class PythonImportCacheItemType { MODULE, TYPE };

struct PythonImportCache;

struct PythonImportCacheItem {
public:
	//! Constructor for a module
	PythonImportCacheItem(const string &name, PythonImportCache &cache)
	    : source(*this), name(name), cache(cache), item_type(PythonImportCacheItemType::MODULE), object(LoadObject()) {
	}
	//! Constructor for an attribute
	PythonImportCacheItem(const string &name, PythonImportCacheItem &source, PythonImportCache &cache)
	    : source(source), name(name), cache(cache), item_type(PythonImportCacheItemType::TYPE), object(LoadObject()) {
	}
	virtual ~PythonImportCacheItem() {
	}

public:
	py::handle operator()(void);
	const string &Name() {
		return name;
	}

private:
	PyObject *AddCache(py::object object);
	PyObject *LoadObject();
	PyObject *LoadModule();
	PyObject *LoadAttribute();

private:
	//! Where to get the parent item from
	PythonImportCacheItem &source;
	//! Name of the object
	string name;
	//! The cache that owns the py::objects;
	PythonImportCache &cache;
	//! Used in LoadObject to determine how to load the object
	PythonImportCacheItemType item_type;
	//! The stored item
	PyObject *object;
};

//===--------------------------------------------------------------------===//
// Modules
//===--------------------------------------------------------------------===//

struct DecimalCacheItem : public PythonImportCacheItem {
public:
	DecimalCacheItem(PythonImportCache &cache)
	    : PythonImportCacheItem("decimal", cache), Decimal("Decimal", *this, cache) {
	}

public:
	//! decimal.Decimal
	PythonImportCacheItem Decimal;
};

struct UUIDCacheItem : public PythonImportCacheItem {
public:
	UUIDCacheItem(PythonImportCache &cache) : PythonImportCacheItem("uuid", cache), UUID("UUID", *this, cache) {
	}

public:
	//! uuid.UUID
	PythonImportCacheItem UUID;
};

struct NumpyCacheItem : public PythonImportCacheItem {
public:
	NumpyCacheItem(PythonImportCache &cache) : PythonImportCacheItem("numpy", cache), ndarray("ndarray", *this, cache) {
	}

public:
	PythonImportCacheItem ndarray;
};

struct DatetimeCacheItem : public PythonImportCacheItem {
public:
	DatetimeCacheItem(PythonImportCache &cache)
	    : PythonImportCacheItem("datetime", cache), datetime("datetime", *this, cache), date("date", *this, cache),
	      time("time", *this, cache) {
	}

public:
	PythonImportCacheItem datetime;
	PythonImportCacheItem date;
	PythonImportCacheItem time;
};

//===--------------------------------------------------------------------===//
// PythonImportCache
//===--------------------------------------------------------------------===//

//! Constructs py::objects, so it takes the gil before doing so, then releases it after initializing all modules
struct PythonImportCache {
public:
	//! The vector and the GIL have to be initialized before the modules are
	//! It's pretty horrible: but this order matters!
	explicit PythonImportCache()
	    : owned_objects(), gil(make_unique<PythonGILWrapper>()), numpy(*this), datetime(*this), decimal(*this),
	      uuid(*this) {
		//! Release the GIL
		gil = nullptr;
	}
	~PythonImportCache();
	//! Stored modules
public:
	// Do not touch this order!
	vector<unique_ptr<RegisteredObject>> owned_objects;
	unique_ptr<PythonGILWrapper> gil;

	NumpyCacheItem numpy;
	DatetimeCacheItem datetime;
	DecimalCacheItem decimal;
	UUIDCacheItem uuid;

public:
	PyObject *AddCache(py::object item);
};

} // namespace duckdb

//===--------------------------------------------------------------------===//
// Extension documentation
//===--------------------------------------------------------------------===//

//! The code below shows an example implementation of an attribute with subtypes

// struct NdArrayCacheItem : public PythonImportCacheItem {
// public:
// NdArrayCacheItem(PythonImportCacheItem& source, PythonImportCache& cache) : PythonImportCacheItem("ndarray", source,
// cache), 		new_object("new_object", *this, cache)
//{}
// public:
// PythonImportCacheItem new_object;
// };
