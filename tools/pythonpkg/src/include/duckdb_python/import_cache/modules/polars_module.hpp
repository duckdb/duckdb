//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/polars_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct PolarsCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "polars";

public:
	~PolarsCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		DataFrame.LoadAttribute("DataFrame", cache, *this);
		LazyFrame.LoadAttribute("LazyFrame", cache, *this);
	}

public:
	//! pandas.DataFrame
	PythonImportCacheItem DataFrame;
	PythonImportCacheItem LazyFrame;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace duckdb
