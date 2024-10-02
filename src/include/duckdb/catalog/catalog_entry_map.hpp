//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
class CatalogEntry;

struct CatalogEntryHashFunction {
	uint64_t operator()(const reference<CatalogEntry> &a) const {
		std::hash<void *> hash_func;
		return hash_func((void *)&a.get());
	}
};

struct CatalogEntryEquality {
	bool operator()(const reference<CatalogEntry> &a, const reference<CatalogEntry> &b) const {
		return RefersToSameObject(a, b);
	}
};

using catalog_entry_set_t = unordered_set<reference<CatalogEntry>, CatalogEntryHashFunction, CatalogEntryEquality>;

template <typename T>
using catalog_entry_map_t = unordered_map<reference<CatalogEntry>, T, CatalogEntryHashFunction, CatalogEntryEquality>;

using catalog_entry_vector_t = vector<reference<CatalogEntry>>;

} // namespace duckdb
