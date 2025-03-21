
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/pyarrow_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct PyarrowDatasetCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "pyarrow.dataset";

public:
	PyarrowDatasetCacheItem()
	    : PythonImportCacheItem("pyarrow.dataset"), Scanner("Scanner", this), Dataset("Dataset", this) {
	}
	~PyarrowDatasetCacheItem() override {
	}

	PythonImportCacheItem Scanner;
	PythonImportCacheItem Dataset;
};

struct PyarrowCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "pyarrow";

public:
	PyarrowCacheItem()
	    : PythonImportCacheItem("pyarrow"), dataset(), Table("Table", this),
	      RecordBatchReader("RecordBatchReader", this), MessageReader("MessageReader", this) {
	}
	~PyarrowCacheItem() override {
	}

	PyarrowDatasetCacheItem dataset;
	PythonImportCacheItem Table;
	PythonImportCacheItem RecordBatchReader;
	PythonImportCacheItem MessageReader;
};

} // namespace duckdb
