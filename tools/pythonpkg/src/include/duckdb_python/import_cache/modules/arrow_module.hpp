//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/arrow_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct ArrowDatasetCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "pyarrow.dataset";
	ArrowDatasetCacheItem()
	    : PythonImportCacheItem("pyarrow.dataset"), Dataset("Dataset", this), Scanner("Scanner", this) {
	}
	~ArrowDatasetCacheItem() override {
	}

public:
	PythonImportCacheItem Dataset;
	PythonImportCacheItem Scanner;

protected:
	bool IsRequired() const final {
		return false;
	}
};

struct ArrowLibCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "pyarrow";
	ArrowLibCacheItem()
	    : PythonImportCacheItem("pyarrow"), dataset(), Table("Table", this),
	      RecordBatchReader("RecordBatchReader", this) {
	}
	~ArrowLibCacheItem() override {
	}

public:
	ArrowDatasetCacheItem dataset;
	PythonImportCacheItem Table;
	PythonImportCacheItem RecordBatchReader;
};

} // namespace duckdb
