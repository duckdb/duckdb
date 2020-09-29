//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
class CatalogSet;

struct Dependency {
	Dependency(CatalogEntry *entry);

	CatalogSet *set;
	idx_t entry_index;

	idx_t Hash() const;
};

struct DependencyHashFunction {
	uint64_t operator()(const Dependency &dep) const {
		return dep.Hash();
	}
};

struct DependencyEquality {
	bool operator()(const Dependency &a, const Dependency &b) const {
		return a.set == b.set && a.entry_index == b.entry_index;
	}
};

template <typename T> using dependency_map_t = unordered_map<Dependency, T, DependencyHashFunction, DependencyEquality>;

using dependency_set_t = unordered_set<Dependency, DependencyHashFunction, DependencyEquality>;

} // namespace duckdb
