
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

struct IpythonDisplayCacheItem : public PythonImportCacheItem {

public:
	IpythonDisplayCacheItem(optional_ptr<PythonImportCacheItem> parent)
	    : PythonImportCacheItem("display", parent), display("display", this) {
	}
	~IpythonDisplayCacheItem() override {
	}

	PythonImportCacheItem display;
};

struct IpythonCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "IPython";

public:
	IpythonCacheItem() : PythonImportCacheItem("IPython"), get_ipython("get_ipython", this), display(this) {
	}
	~IpythonCacheItem() override {
	}

	PythonImportCacheItem get_ipython;
	IpythonDisplayCacheItem display;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace duckdb
