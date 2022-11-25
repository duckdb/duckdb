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

struct ArrowLibCacheItem : public PythonImportCacheItem {
public:
	~ArrowLibCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		Table.LoadAttribute("Table", cache, *this);
		RecordBatchReader.LoadAttribute("RecordBatchReader", cache, *this);
	}

public:
	PythonImportCacheItem Table;
	PythonImportCacheItem RecordBatchReader;
};

struct ArrowDatasetCacheItem : public PythonImportCacheItem {
public:
	~ArrowDatasetCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		Dataset.LoadAttribute("Dataset", cache, *this);
		Scanner.LoadAttribute("Scanner", cache, *this);
	}

public:
	PythonImportCacheItem Dataset;
	PythonImportCacheItem Scanner;
};

struct ArrowCacheItem : public PythonImportCacheItem {
public:
	~ArrowCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		lib.LoadAttribute("lib", cache, *this);
		dataset.LoadModule("pyarrow.dataset", cache);
	}

public:
	ArrowLibCacheItem lib;
	ArrowDatasetCacheItem dataset;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace duckdb
