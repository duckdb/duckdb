
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/collections_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct CollectionsAbcCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "collections.abc";

public:
	CollectionsAbcCacheItem()
	    : PythonImportCacheItem("collections.abc"), Iterable("Iterable", this), Mapping("Mapping", this) {
	}
	~CollectionsAbcCacheItem() override {
	}

	PythonImportCacheItem Iterable;
	PythonImportCacheItem Mapping;
};

struct CollectionsCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "collections";

public:
	CollectionsCacheItem() : PythonImportCacheItem("collections"), abc() {
	}
	~CollectionsCacheItem() override {
	}

	CollectionsAbcCacheItem abc;
};

} // namespace duckdb
