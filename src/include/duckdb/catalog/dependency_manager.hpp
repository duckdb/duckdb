//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/dependency.hpp"

namespace duckdb {
class Catalog;
class Transaction;

//! The DependencyManager is in charge of managing dependencies between catalog entries
class DependencyManager {
	friend class CatalogSet;

public:
	DependencyManager(Catalog &catalog);

	//! Erase the object from the DependencyManager; this should only happen when the object itself is destroyed
	void EraseObject(CatalogEntry *object);
	//! Clear all the dependencies of all entries in the catalog set
	void ClearDependencies(CatalogSet &set);
	//! Verify that there are no dependencies on the object so that it can be altered
	void AlterObject(Transaction &transaction, CatalogEntry *object);

private:
	Catalog &catalog;
	//! Map of objects that DEPEND on [object], i.e. [object] can only be deleted when all entries in the dependency map
	//! are deleted.
	dependency_map_t<dependency_set_t> dependents_map;
	//! Map of objects that the source object DEPENDS on, i.e. when any of the entries in the vector perform a CASCADE
	//! drop then [object] is deleted as wel
	dependency_map_t<dependency_set_t> dependencies_map;
	//! The amount of times a specific dependency
	dependency_map_t<idx_t> reference_count;

private:
	void AddObject(Transaction &transaction, CatalogEntry *object, unordered_set<CatalogEntry *> &dependencies);
	void DropObject(Transaction &transaction, CatalogEntry *object, bool cascade, set_lock_map_t &lock_set);
	void EraseObjectInternal(CatalogEntry *object);
};

} // namespace duckdb
