
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

struct PytzCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "pytz";

public:
	PytzCacheItem() : PythonImportCacheItem("pytz"), timezone("timezone", this) {
	}
	~PytzCacheItem() override {
	}

	PythonImportCacheItem timezone;
};

} // namespace duckdb
