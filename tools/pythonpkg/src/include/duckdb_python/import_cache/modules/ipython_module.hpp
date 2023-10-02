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
	~IPythonDisplayCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		display.LoadAttribute("display", cache, *this);
	}

public:
	PythonImportCacheItem display;
};

struct IPythonCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "IPython";

public:
	~IPythonCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		get_ipython.LoadAttribute("get_ipython", cache, *this);
		display.LoadModule("IPython.display", cache);
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
