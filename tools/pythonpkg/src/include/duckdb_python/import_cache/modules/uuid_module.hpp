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
	~UUIDCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		UUID.LoadAttribute("UUID", cache, *this);
	}

public:
	//! uuid.UUID
	PythonImportCacheItem UUID;
};

} // namespace duckdb
