//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/pandas_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct PandasLibsCacheItem : public PythonImportCacheItem {
public:
	~PandasLibsCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		NAType.LoadAttribute("NAType", cache, *this);
	}

public:
	PythonImportCacheItem NAType;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

struct PandasCacheItem : public PythonImportCacheItem {
public:
	~PandasCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		DataFrame.LoadAttribute("DataFrame", cache, *this);
		libs.LoadModule("pandas._libs.missing", cache);
	}

public:
	//! pandas.DataFrame
	PythonImportCacheItem DataFrame;
	PandasLibsCacheItem libs;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace duckdb
