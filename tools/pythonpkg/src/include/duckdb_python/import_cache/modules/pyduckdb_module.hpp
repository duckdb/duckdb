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
	static constexpr const char *Name = "duckdb.filesystem";

public:
	PyDuckDBFileSystemCacheItem()
	    : PythonImportCacheItem("duckdb.filesystem"), ModifiedMemoryFileSystem("ModifiedMemoryFileSystem", this) {
	}
	~PyDuckDBFileSystemCacheItem() override {
	}

protected:
	bool IsRequired() const override {
		return false;
	}

public:
	PythonImportCacheItem ModifiedMemoryFileSystem;
};

struct PyDuckDBCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "duckdb";

public:
	PyDuckDBCacheItem() : PythonImportCacheItem("duckdb"), filesystem(), Value("Value", this) {
	}
	~PyDuckDBCacheItem() override {
	}

public:
	PyDuckDBFileSystemCacheItem filesystem;
	PythonImportCacheItem Value;
};

} // namespace duckdb
