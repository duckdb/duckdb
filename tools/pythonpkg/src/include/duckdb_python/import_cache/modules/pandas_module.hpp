
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

struct PandasLibsMissingCacheItem : public PythonImportCacheItem {

public:
	PandasLibsMissingCacheItem(optional_ptr<PythonImportCacheItem> parent)
	    : PythonImportCacheItem("missing", parent), NAType("NAType", this) {
	}
	~PandasLibsMissingCacheItem() override {
	}

	PythonImportCacheItem NAType;
};

struct PandasLibsCacheItem : public PythonImportCacheItem {

public:
	PandasLibsCacheItem(optional_ptr<PythonImportCacheItem> parent)
	    : PythonImportCacheItem("_libs", parent), missing(this) {
	}
	~PandasLibsCacheItem() override {
	}

	PandasLibsMissingCacheItem missing;

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
	    : PythonImportCacheItem("pandas"), DataFrame("DataFrame", this), _libs(this), isnull("isnull", this),
	      ArrowDtype("ArrowDtype", this) {
	}
	~PandasCacheItem() override {
	}

	PythonImportCacheItem DataFrame;
	PandasLibsCacheItem _libs;
	PythonImportCacheItem isnull;
	PythonImportCacheItem ArrowDtype;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace duckdb
