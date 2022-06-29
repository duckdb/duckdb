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

#include "datetime.h" //From python

namespace duckdb {

enum class PythonImportCacheItemType { MODULE, TYPE };

struct PythonImportCache;

// Every cache item knows how to retrieve its object
struct PythonImportCacheItem {
public:
	//! Constructor for a module
	PythonImportCacheItem(const string &name, PythonImportCache &cache)
	    : source(*this), object(nullptr), name(name), cache(cache), item_type(PythonImportCacheItemType::MODULE) {
	}
	//! Constructor for an attribute
	PythonImportCacheItem(const string &name, PythonImportCacheItem &source, PythonImportCache &cache)
	    : source(source), object(nullptr), name(name), cache(cache), item_type(PythonImportCacheItemType::TYPE) {
	}
	virtual ~PythonImportCacheItem() {
	}

public:
	py::handle operator()(void);
	const string &Name() {
		return name;
	}

protected:
	//! Where to get the parent item from
	PythonImportCacheItem &source;
	//! The stored item
	PyObject *object;

private:
	PyObject *AddCache(py::object object);
	PyObject *LoadObject();
	PyObject *LoadModule();
	PyObject *LoadAttribute();

private:
	//! Name of the object
	string name;
	//! The cache that owns the py::objects;
	PythonImportCache &cache;
	//! Used in LoadObject to determine how to load the object
	PythonImportCacheItemType item_type;
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

struct PythonImportCache {
public:
	PythonImportCache() : numpy(*this), datetime(*this), decimal(*this), uuid(*this), owned_objects() {
	}
	~PythonImportCache();
	//! Stored modules
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
