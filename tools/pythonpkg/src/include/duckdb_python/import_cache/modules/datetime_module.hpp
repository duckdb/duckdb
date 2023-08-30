//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/datetime_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct DatetimeDatetimeCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "datetime";

public:
	DatetimeDatetimeCacheItem(optional_ptr<PythonImportCacheItem> parent)
	    : PythonImportCacheItem("datetime", parent), max("max", this), min("min", this) {
	}
	~DatetimeDatetimeCacheItem() override {
	}

public:
	PythonImportCacheItem max;
	PythonImportCacheItem min;
};

struct DatetimeDateCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "date";

public:
	DatetimeDateCacheItem(optional_ptr<PythonImportCacheItem> parent)
	    : PythonImportCacheItem("date", parent), max("max", this), min("min", this) {
	}
	~DatetimeDateCacheItem() override {
	}

public:
	PythonImportCacheItem max;
	PythonImportCacheItem min;
};

struct DatetimeCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "datetime";

public:
	DatetimeCacheItem()
	    : PythonImportCacheItem("datetime"), datetime(this), date(this), time("time", this),
	      timedelta("timedelta", this) {
	}
	~DatetimeCacheItem() override {
	}

public:
	DatetimeDatetimeCacheItem datetime;
	DatetimeDateCacheItem date;
	PythonImportCacheItem time;
	PythonImportCacheItem timedelta;
};

} // namespace duckdb
