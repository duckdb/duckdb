
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
	      timezone("timezone", this), datetime(this) {
	}
	~DatetimeCacheItem() override {
	}

	DatetimeDateCacheItem date;
	PythonImportCacheItem time;
	PythonImportCacheItem timedelta;
	PythonImportCacheItem timezone;
	DatetimeDatetimeCacheItem datetime;
};

} // namespace duckdb
