
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/ipython_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

//! Note: This class is generated using scripts.
//! If you need to add a new object to the cache you must:
//! 1. adjust tools/pythonpkg/scripts/imports.py
//! 2. run python3 tools/pythonpkg/scripts/generate_import_cache_json.py
//! 3. run python3 tools/pythonpkg/scripts/generate_import_cache_cpp.py
//! 4. run make format-main (the generator doesn't respect the formatting rules ;))

namespace duckdb {

struct IpythonDisplayCacheItem : public PythonImportCacheItem {

public:
	IpythonDisplayCacheItem(optional_ptr<PythonImportCacheItem> parent)
	    : PythonImportCacheItem("display", parent), display("display", this), HTML("HTML", this) {
	}
	~IpythonDisplayCacheItem() override {
	}

	PythonImportCacheItem display;
	PythonImportCacheItem HTML;
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
