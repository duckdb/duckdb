
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/pyduckdb_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct PyduckdbFilesystemCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "pyduckdb.filesystem";

public:
	PyduckdbFilesystemCacheItem()
	    : PythonImportCacheItem("pyduckdb.filesystem"), ModifiedMemoryFileSystem("ModifiedMemoryFileSystem", this) {
	}
	~PyduckdbFilesystemCacheItem() override {
	}

	PythonImportCacheItem ModifiedMemoryFileSystem;
};

struct PyduckdbCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "pyduckdb";

public:
	PyduckdbCacheItem() : PythonImportCacheItem("pyduckdb"), filesystem(), Value("Value", this) {
	}
	~PyduckdbCacheItem() override {
	}

	PyduckdbFilesystemCacheItem filesystem;
	PythonImportCacheItem Value;
};

} // namespace duckdb
