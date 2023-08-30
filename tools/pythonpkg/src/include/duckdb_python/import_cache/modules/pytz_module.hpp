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
	PyTzCacheItem() : PythonImportCacheItem("pytz"), timezone("timezone", this) {
	}
	~PyTzCacheItem() override {
	}

public:
	//! pytz.timezone
	PythonImportCacheItem timezone;
};

} // namespace duckdb
