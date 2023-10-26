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
#include "duckdb/catalog/catalog_entry/dependency_set_catalog_entry.hpp"

#include <functional>

namespace duckdb {
class DuckCatalog;
class ClientContext;
class DependencyList;
class DependencyCatalogEntry;

//! The DependencyManager is in charge of managing dependencies between catalog entries
class DependencyManager {
	friend class CatalogSet;

public:
	explicit DependencyManager(DuckCatalog &catalog);

	//! Erase the object from the DependencyManager; this should only happen when the object itself is destroyed
	void EraseObject(CatalogEntry &object);

	//! Scans all dependencies, returning pairs of (object, dependent)
	void Scan(ClientContext &context,
	          const std::function<void(CatalogEntry &, CatalogEntry &, DependencyType)> &callback);

	void AddOwnership(CatalogTransaction transaction, CatalogEntry &owner, CatalogEntry &entry);

private:
	DuckCatalog &catalog;
	CatalogSet connections;

private:
	bool IsDependencyEntry(CatalogEntry &entry) const;
	DependencySetCatalogEntry &GetOrCreateDependencySet(CatalogTransaction transaction, CatalogEntry &entry);
	optional_ptr<DependencySetCatalogEntry> GetDependencySet(CatalogTransaction transaction, CatalogEntry &entry);
	optional_ptr<DependencySetCatalogEntry> GetDependencySet(CatalogEntry &entry);
	void DropObjectInternalNew(CatalogTransaction transaction, CatalogEntry &object, bool cascade);
	void AlterObjectInternalNew(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj);

	using lookup_callback_t = std::function<void(optional_ptr<CatalogEntry> entry, optional_ptr<CatalogSet> set,
	                                             optional_ptr<MappingValue> mapping)>;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, CatalogEntry &dependency,
	                                       lookup_callback_t callback);

private:
	void AddObject(CatalogTransaction transaction, CatalogEntry &object, DependencyList &dependencies);
	void DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade);
	void AlterObject(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj);
	void EraseObjectInternal(CatalogEntry &object);
};

} // namespace duckdb
