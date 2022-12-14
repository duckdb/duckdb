//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
class CatalogEntry;

//! The DependencyList
class DependencyList {
	friend class DependencyManager;

public:
	DUCKDB_API void AddDependency(CatalogEntry *entry);

private:
	unordered_set<CatalogEntry *> set;
};
} // namespace duckdb
