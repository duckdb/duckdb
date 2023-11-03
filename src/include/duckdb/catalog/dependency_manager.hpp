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
#include "duckdb/common/stack.hpp"

#include <functional>

namespace duckdb {
class DuckCatalog;
class ClientContext;
class DependencyList;
class DependencyCatalogEntry;
class DependencySetCatalogEntry;
class LogicalDependencyList;

//! The DependencyManager is in charge of managing dependencies between catalog entries
class DependencyManager {
	friend class CatalogSet;
	friend class DependencySetCatalogEntry;

public:
	explicit DependencyManager(DuckCatalog &catalog);

	//! Scans all dependencies, returning pairs of (object, dependent)
	void Scan(ClientContext &context,
	          const std::function<void(CatalogEntry &, CatalogEntry &, DependencyFlags)> &callback);

	void AddOwnership(CatalogTransaction transaction, CatalogEntry &owner, CatalogEntry &entry);
	optional_ptr<DependencySetCatalogEntry> GetDependencySet(optional_ptr<CatalogTransaction> transaction,
	                                                         const string &mangled_name);
	optional_ptr<DependencySetCatalogEntry> GetDependencySet(optional_ptr<CatalogTransaction> transaction,
	                                                         const LogicalDependency &entry);
	DependencySetCatalogEntry &GetOrCreateDependencySet(CatalogTransaction transaction, const LogicalDependency &entry);

	//! Get the order of entries needed by EXPORT, the objects with no dependencies are exported first
	catalog_entry_vector_t GetExportOrder(optional_ptr<CatalogTransaction> transaction = nullptr);

	catalog_entry_vector_t GetDependencySets(optional_ptr<CatalogTransaction> transaction);

private:
	DuckCatalog &catalog;
	CatalogSet dependency_sets;

private:
	bool IsSystemEntry(CatalogEntry &entry) const;
	DependencySetCatalogEntry &GetOrCreateDependencySet(CatalogTransaction transaction, CatalogEntry &entry);
	void DropDependencySet(CatalogTransaction, CatalogEntry &entry);
	optional_ptr<DependencySetCatalogEntry> GetDependencySet(optional_ptr<CatalogTransaction> transaction,
	                                                         CatalogEntry &entry);

	optional_ptr<CatalogEntry> LookupEntry(optional_ptr<CatalogTransaction> transaction,
	                                       const LogicalDependency &dependency);
	optional_ptr<CatalogEntry> LookupEntry(optional_ptr<CatalogTransaction> transaction, CatalogEntry &dependency);

	void CleanupDependencies(CatalogTransaction transaction, CatalogEntry &entry);

public:
	static string GetSchema(CatalogEntry &entry);
	static string MangleName(CatalogType type, const string &schema, const string &name);
	static string MangleName(CatalogEntry &entry);
	static string MangleName(const LogicalDependency &entry);

private:
	void AddObject(CatalogTransaction transaction, CatalogEntry &object, const LogicalDependencyList &dependencies);
	void DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade);
	void AlterObject(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj);

	DependencySetCatalogEntry &LookupSet(optional_ptr<CatalogTransaction> transaction, CatalogEntry &entry);
};

} // namespace duckdb
