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

struct TypingCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "typing";

public:
	~TypingCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		_UnionGenericAlias.LoadAttribute("_UnionGenericAlias", cache, *this);
	}

public:
	PythonImportCacheItem _UnionGenericAlias;
};

} // namespace duckdb
