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
	PyDuckDBFileSystemCacheItem()
	    : PythonImportCacheItem("pyduckdb.filesystem"), ModifiedMemoryFileSystem("ModifiedMemoryFileSystem", this) {
	}
	~PyDuckDBFileSystemCacheItem() override {
	}

public:
	PythonImportCacheItem ModifiedMemoryFileSystem;
};

struct PyDuckDBCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "pyduckdb";

public:
	PyDuckDBCacheItem() : PythonImportCacheItem("pyduckdb"), filesystem(), Value("Value", this) {
	}
	~PyDuckDBCacheItem() override {
	}

public:
	PyDuckDBFileSystemCacheItem filesystem;
	PythonImportCacheItem Value;
};

} // namespace duckdb
