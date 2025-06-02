
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/ipywidgets_module.hpp
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

struct IpywidgetsCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "ipywidgets";

public:
	IpywidgetsCacheItem() : PythonImportCacheItem("ipywidgets"), FloatProgress("FloatProgress", this) {
	}
	~IpywidgetsCacheItem() override {
	}

	PythonImportCacheItem FloatProgress;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace duckdb
