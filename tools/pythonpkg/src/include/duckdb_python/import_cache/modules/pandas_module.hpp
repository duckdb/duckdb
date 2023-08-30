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

// pandas.libs
struct PandasLibsCacheItem : public PythonImportCacheItem {
public:
	PandasLibsCacheItem(optional_ptr<PythonImportCacheItem> parent)
	    : PythonImportCacheItem("pandas._libs.missing"), NAType("NAType", this) {
	}
	~PandasLibsCacheItem() override {
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
	PandasCacheItem()
	    : PythonImportCacheItem("pandas"), DataFrame("DataFrame", this), libs(this), isnull("isnull", this),
	      ArrowDtype("ArrowDtype", this) {
	}

public:
	//! pandas.DataFrame
	PythonImportCacheItem DataFrame;
	PandasLibsCacheItem libs;
	PythonImportCacheItem isnull;
	PythonImportCacheItem ArrowDtype;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace duckdb
