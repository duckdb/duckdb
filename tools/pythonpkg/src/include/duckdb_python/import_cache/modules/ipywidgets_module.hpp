
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/ipywidgets_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct IpywidgetsCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "ipywidgets";

public:
	IpywidgetsCacheItem() : PythonImportCacheItem("ipywidgets"), FloatProgress("FloatProgress", this) {
	}
	~IpywidgetsCacheItem() override {
	}

	PythonImportCacheItem FloatProgress;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace duckdb
