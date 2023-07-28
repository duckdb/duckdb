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
	static constexpr const char *Name = "datetime.datetime";

public:
	~DatetimeDatetimeCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		max.LoadAttribute("max", cache, *this);
		min.LoadAttribute("min", cache, *this);
	}

public:
	PythonImportCacheItem max;
	PythonImportCacheItem min;
};

struct DatetimeDateCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "datetime.date";

public:
	~DatetimeDateCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		max.LoadAttribute("max", cache, *this);
		min.LoadAttribute("min", cache, *this);
	}

public:
	PythonImportCacheItem max;
	PythonImportCacheItem min;
};

struct DatetimeCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "datetime";

public:
	~DatetimeCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		datetime.LoadAttribute("datetime", cache, *this);
		date.LoadAttribute("date", cache, *this);
		time.LoadAttribute("time", cache, *this);
		timedelta.LoadAttribute("timedelta", cache, *this);
	}

public:
	DatetimeDatetimeCacheItem datetime;
	DatetimeDateCacheItem date;
	PythonImportCacheItem time;
	PythonImportCacheItem timedelta;
};

} // namespace duckdb
