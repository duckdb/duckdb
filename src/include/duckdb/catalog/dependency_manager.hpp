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
#include "duckdb/catalog/catalog_entry_map.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"

#include <functional>

namespace duckdb {
class DuckCatalog;
class ClientContext;
class PhysicalDependencyList;

//! The DependencyManager is in charge of managing dependencies between catalog entries
class DependencyManager {
	friend class CatalogSet;

public:
	explicit DependencyManager(DuckCatalog &catalog);

	//! Erase the object from the DependencyManager; this should only happen when the object itself is destroyed
	void EraseObject(CatalogEntry &object);

	//! Scans all dependencies, returning pairs of (object, dependent)
	void Scan(const std::function<void(CatalogEntry &, CatalogEntry &, DependencyType)> &callback);

	void AddOwnership(CatalogTransaction transaction, CatalogEntry &owner, CatalogEntry &entry);

	//! Get the order of entries needed by EXPORT, the objects with no dependencies are exported first
	catalog_entry_vector_t GetExportOrder();

private:
	DuckCatalog &catalog;
	//! Map of objects that DEPEND on [object], i.e. [object] can only be deleted when all entries in the dependency map
	//! are deleted.
	catalog_entry_map_t<dependency_set_t> dependents_map;
	//! Map of objects that the source object DEPENDS on, i.e. when any of the entries in the vector perform a CASCADE
	//! drop then [object] is deleted as well
	catalog_entry_map_t<catalog_entry_set_t> dependencies_map;

private:
	void AddObject(CatalogTransaction transaction, CatalogEntry &object, PhysicalDependencyList &dependencies);
	void DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade);
	void AlterObject(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj);
	void EraseObjectInternal(CatalogEntry &object);

	optional_ptr<dependency_set_t> GetEntriesThatDependOnObject(CatalogEntry &object);
	optional_ptr<catalog_entry_set_t> GetEntriesThatObjectDependsOn(CatalogEntry &object);
	bool AllExportDependenciesWritten(CatalogEntry &object, optional_ptr<catalog_entry_set_t> dependencies,
	                                  catalog_entry_set_t &exported);

	void PrintDependencyMap();
	void PrintDependentsMap();
};
} // namespace duckdb
