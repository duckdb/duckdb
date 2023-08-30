//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/ipython_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct IPythonDisplayCacheItem : public PythonImportCacheItem {
public:
	IPythonDisplayCacheItem(optional_ptr<PythonImportCacheItem> parent)
	    : PythonImportCacheItem("display", parent), display("display", this) {
	}
	~IPythonDisplayCacheItem() override {
	}

public:
	PythonImportCacheItem display;
};

struct IPythonCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "IPython";

public:
	IPythonCacheItem() : PythonImportCacheItem("IPython"), get_ipython("get_ipython", this), display(this) {
	}

	~IPythonCacheItem() override {
	}

public:
	PythonImportCacheItem get_ipython;
	IPythonDisplayCacheItem display;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace duckdb
