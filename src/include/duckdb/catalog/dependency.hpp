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

enum class DependencyType {
	DEPENDENCY_REGULAR = 0,
	DEPENDENCY_AUTOMATIC = 1,
	DEPENDENCY_OWNS = 2,
	DEPENDENCY_OWNED_BY = 3
};

struct Dependency {
	Dependency(CatalogEntry &entry, DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR)
	    : // NOLINT: Allow implicit conversion from `CatalogEntry`
	      entry(entry), dependency_type(dependency_type) {
	}

	//! The catalog entry this depends on
	reference<CatalogEntry> entry;
	//! The type of dependency
	DependencyType dependency_type;
};

struct DependencyHashFunction {
	uint64_t operator()(const Dependency &a) const {
		std::hash<void *> hash_func;
		return hash_func((void *)&a.entry.get());
	}
};

struct DependencyEquality {
	bool operator()(const Dependency &a, const Dependency &b) const {
		return RefersToSameObject(a.entry, b.entry);
	}
};
using dependency_set_t = unordered_set<Dependency, DependencyHashFunction, DependencyEquality>;

} // namespace duckdb
