//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
class CatalogEntry;

struct Dependency {
	Dependency(CatalogEntry *entry, bool requires_cascade = true) :
		entry(entry), requires_cascade(requires_cascade) {}

	//! The catalog entry this depends on
	CatalogEntry *entry;
	//! Whether or not this dependency requires a cascade to drop
	bool requires_cascade;
};


struct DependencyHashFunction {
	uint64_t operator()(const Dependency &a) const {
		std::hash<void*> hash_func;
		return hash_func((void*)a.entry);
	}
};

struct DependencyEquality {
	bool operator()(const Dependency &a, const Dependency &b) const {
		return a.entry == b.entry;
	}
};

using dependency_set_t = unordered_set<Dependency, DependencyHashFunction, DependencyEquality>;

} // namespace duckdb
