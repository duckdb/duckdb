
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/uuid_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct UuidCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "uuid";

public:
	UuidCacheItem() : PythonImportCacheItem("uuid"), UUID("UUID", this) {
	}
	~UuidCacheItem() override {
	}

	PythonImportCacheItem UUID;
};

} // namespace duckdb
