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
	static constexpr const char *Name = "pyarrow";
	~ArrowLibCacheItem() override {
	}
	void LoadSubtypes(PythonImportCache &cache) override {
		Table.LoadAttribute("Table", cache, *this);
		RecordBatchReader.LoadAttribute("RecordBatchReader", cache, *this);
	}

public:
	PythonImportCacheItem Table;
	PythonImportCacheItem RecordBatchReader;
};

struct ArrowDatasetCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "pyarrow.dataset";
	~ArrowDatasetCacheItem() override {
	}
	void LoadSubtypes(PythonImportCache &cache) override {
		Dataset.LoadAttribute("Dataset", cache, *this);
		Scanner.LoadAttribute("Scanner", cache, *this);
	}

public:
	PythonImportCacheItem Dataset;
	PythonImportCacheItem Scanner;

protected:
	bool IsRequired() const final {
		return false;
	}
};

} // namespace duckdb
