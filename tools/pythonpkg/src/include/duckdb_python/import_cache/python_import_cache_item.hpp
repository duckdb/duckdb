//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/python_import_cache_item.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct PythonImportCache;

struct PythonImportCacheItem {
public:
	PythonImportCacheItem() : load_succeeded(false), object(nullptr) {
	}
	virtual ~PythonImportCacheItem() {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) {
	}

public:
	bool LoadSucceeded() const;
	bool IsLoaded() const;
	py::handle operator()(void) const;
	void LoadModule(const string &name, PythonImportCache &cache);
	void LoadAttribute(const string &name, PythonImportCache &cache, PythonImportCacheItem &source);

protected:
	virtual bool IsRequired() const {
		return true;
	}

private:
	py::handle AddCache(PythonImportCache &cache, py::object object);

private:
	//! Whether or not we attempted to load the module
	bool load_succeeded;
	//! The stored item
	py::handle object;
};

} // namespace duckdb
