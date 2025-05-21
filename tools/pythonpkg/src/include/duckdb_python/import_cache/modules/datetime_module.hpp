
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/datetime_module.hpp
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

struct DatetimeDatetimeCacheItem : public PythonImportCacheItem {

public:
	DatetimeDatetimeCacheItem(optional_ptr<PythonImportCacheItem> parent)
	    : PythonImportCacheItem("datetime", parent), min("min", this), max("max", this), combine("combine", this) {
	}
	~DatetimeDatetimeCacheItem() override {
	}

	PythonImportCacheItem min;
	PythonImportCacheItem max;
	PythonImportCacheItem combine;
};

struct DatetimeDateCacheItem : public PythonImportCacheItem {

public:
	DatetimeDateCacheItem(optional_ptr<PythonImportCacheItem> parent)
	    : PythonImportCacheItem("date", parent), max("max", this), min("min", this) {
	}
	~DatetimeDateCacheItem() override {
	}

	PythonImportCacheItem max;
	PythonImportCacheItem min;
};

struct DatetimeCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "datetime";

public:
	DatetimeCacheItem()
	    : PythonImportCacheItem("datetime"), date(this), time("time", this), timedelta("timedelta", this),
	      datetime(this), timezone("timezone", this) {
	}
	~DatetimeCacheItem() override {
	}

	DatetimeDateCacheItem date;
	PythonImportCacheItem time;
	PythonImportCacheItem timedelta;
	DatetimeDatetimeCacheItem datetime;
	PythonImportCacheItem timezone;
};

} // namespace duckdb
