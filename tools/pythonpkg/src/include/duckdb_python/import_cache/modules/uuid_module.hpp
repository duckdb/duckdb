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

struct UUIDCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "uuid";

public:
	UUIDCacheItem() : PythonImportCacheItem("uuid"), UUID("UUID", this) {
	}
	~UUIDCacheItem() override {
	}

public:
	//! uuid.UUID
	PythonImportCacheItem UUID;
};

} // namespace duckdb
