//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry_map.hpp"

namespace duckdb {
class Catalog;
class CatalogEntry;

//! The DependencyList
class DependencyList {
	friend class DependencyManager;

public:
	DUCKDB_API void AddDependency(CatalogEntry &entry);

	DUCKDB_API void VerifyDependencies(Catalog &catalog, const string &name);

private:
	catalog_entry_set_t set;
};
} // namespace duckdb
