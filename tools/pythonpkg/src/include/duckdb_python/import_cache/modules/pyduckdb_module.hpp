//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/numpy_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct PyDuckDBFileSystemCacheItem : public PythonImportCacheItem {
	static constexpr const char *Name = "pyduckdb.filesystem";

public:
	~PyDuckDBFileSystemCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		modified_memory_filesystem.LoadAttribute("ModifiedMemoryFileSystem", cache, *this);
	}

public:
	PythonImportCacheItem modified_memory_filesystem;
};

struct PyDuckDBCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "pyduckdb";

public:
	~PyDuckDBCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		filesystem.LoadModule("pyduckdb.filesystem", cache);
		value.LoadAttribute("Value", cache, *this);
	}

public:
	PyDuckDBFileSystemCacheItem filesystem;
	PythonImportCacheItem value;
};

} // namespace duckdb
