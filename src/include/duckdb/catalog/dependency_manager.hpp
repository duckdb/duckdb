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
#include "duckdb/catalog/catalog_transaction.hpp"

#include <functional>

namespace duckdb {
class Catalog;
class ClientContext;
class DependencyList;

//! The DependencyManager is in charge of managing dependencies between catalog entries
class DependencyManager {
	friend class CatalogSet;

public:
	explicit DependencyManager(Catalog &catalog);

	//! Erase the object from the DependencyManager; this should only happen when the object itself is destroyed
	void EraseObject(CatalogEntry *object);

	//! Scans all dependencies, returning pairs of (object, dependent)
	void Scan(const std::function<void(CatalogEntry *, CatalogEntry *, DependencyType)> &callback);

	void AddOwnership(CatalogTransaction transaction, CatalogEntry *owner, CatalogEntry *entry);

private:
	Catalog &catalog;
	//! Map of objects that DEPEND on [object], i.e. [object] can only be deleted when all entries in the dependency map
	//! are deleted.
	unordered_map<CatalogEntry *, dependency_set_t> dependents_map;
	//! Map of objects that the source object DEPENDS on, i.e. when any of the entries in the vector perform a CASCADE
	//! drop then [object] is deleted as well
	unordered_map<CatalogEntry *, unordered_set<CatalogEntry *>> dependencies_map;

private:
	void AddObject(CatalogTransaction transaction, CatalogEntry *object, DependencyList &dependencies);
	void DropObject(CatalogTransaction transaction, CatalogEntry *object, bool cascade);
	void AlterObject(CatalogTransaction transaction, CatalogEntry *old_obj, CatalogEntry *new_obj);
	void EraseObjectInternal(CatalogEntry *object);
};
} // namespace duckdb
