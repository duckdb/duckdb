
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/pyarrow_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

//! Note: This class is generated using scripts.
//! If you need to add a new object to the cache you must:
//! 1. adjust tools/pythonpkg/scripts/imports.py
//! 2. run python3 tools/pythonpkg/scripts/generate_import_cache_json.py
//! 3. run python3 tools/pythonpkg/scripts/generate_import_cache_cpp.py
//! 4. run make format-main (the generator doesn't respect the formatting rules ;))

namespace duckdb {

struct PyarrowIpcCacheItem : public PythonImportCacheItem {

public:
	PyarrowIpcCacheItem(optional_ptr<PythonImportCacheItem> parent)
	    : PythonImportCacheItem("ipc", parent), MessageReader("MessageReader", this) {
	}
	~PyarrowIpcCacheItem() override {
	}

	PythonImportCacheItem MessageReader;
};

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
	      RecordBatchReader("RecordBatchReader", this), ipc(this) {
	}
	~PyarrowCacheItem() override {
	}

	PyarrowDatasetCacheItem dataset;
	PythonImportCacheItem Table;
	PythonImportCacheItem RecordBatchReader;
	PyarrowIpcCacheItem ipc;
};

} // namespace duckdb
