//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/types_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct TypesCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "types";

public:
	~TypesCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		UnionType.LoadAttribute("UnionType", cache, *this);
		GenericAlias.LoadAttribute("GenericAlias", cache, *this);
	}

public:
	PythonImportCacheItem UnionType;
	PythonImportCacheItem GenericAlias;
};

} // namespace duckdb
