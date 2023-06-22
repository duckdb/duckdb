//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/pytz_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct PyTzCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "pytz";

public:
	~PyTzCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		timezone.LoadAttribute("timezone", cache, *this);
	}

public:
	//! pytz.timezone
	PythonImportCacheItem timezone;
};

} // namespace duckdb
