
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/typing_module.hpp
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
	TypingCacheItem() : PythonImportCacheItem("typing"), _UnionGenericAlias("_UnionGenericAlias", this) {
	}
	~TypingCacheItem() override {
	}

	PythonImportCacheItem _UnionGenericAlias;
};

} // namespace duckdb
