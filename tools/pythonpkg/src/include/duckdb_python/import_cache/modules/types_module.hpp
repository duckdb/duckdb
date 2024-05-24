
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
	TypesCacheItem()
	    : PythonImportCacheItem("types"), UnionType("UnionType", this), GenericAlias("GenericAlias", this),
	      BuiltinFunctionType("BuiltinFunctionType", this) {
	}
	~TypesCacheItem() override {
	}

	PythonImportCacheItem UnionType;
	PythonImportCacheItem GenericAlias;
	PythonImportCacheItem BuiltinFunctionType;
};

} // namespace duckdb
