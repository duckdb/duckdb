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

// pandas.core.arrays.arrow.dtype
struct PandasCoreArraysArrowDtypeCacheItem : public PythonImportCacheItem {
public:
	~PandasCoreArraysArrowDtypeCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		ArrowDtype.LoadAttribute("ArrowDtype", cache, *this);
	}

public:
	PythonImportCacheItem ArrowDtype;
};

// pandas.core.arrays.arrow
struct PandasCoreArraysArrowCacheItem : public PythonImportCacheItem {
public:
	~PandasCoreArraysArrowCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		dtype.LoadModule("pandas.core.arrays.arrow.dtype", cache);
	}

public:
	PandasCoreArraysArrowDtypeCacheItem dtype;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

// pandas.core.arrays
struct PandasCoreArraysCacheItem : public PythonImportCacheItem {
public:
	~PandasCoreArraysCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		arrow.LoadModule("pandas.core.arrays.arrow", cache);
	}

public:
	PandasCoreArraysArrowCacheItem arrow;
};

// pandas.core
struct PandasCoreCacheItem : public PythonImportCacheItem {
public:
	~PandasCoreCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		arrays.LoadModule("pandas.core.arrays", cache);
	}

public:
	PandasCoreArraysCacheItem arrays;
};

// pandas.libs
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
	static constexpr const char *Name = "pandas";

public:
	~PandasCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		DataFrame.LoadAttribute("DataFrame", cache, *this);
		libs.LoadModule("pandas._libs.missing", cache);
		core.LoadModule("pandas.core", cache);
		isnull.LoadAttribute("isnull", cache, *this);
	}

public:
	//! pandas.DataFrame
	PythonImportCacheItem DataFrame;
	PandasLibsCacheItem libs;
	PandasCoreCacheItem core;
	PythonImportCacheItem isnull;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace duckdb
