//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/numpy_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct BuiltinsCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "builtins";

public:
	~BuiltinsCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		str.LoadAttribute("str", cache, *this);
	}

public:
	PythonImportCacheItem str;
};

} // namespace duckdb
