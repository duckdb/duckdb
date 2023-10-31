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

// struct ExportDependencies {
// public:
//	ExportDependencies(catalog_entry_map_t<dependency_set_t> &dependents,
//	                   catalog_entry_map_t<catalog_entry_set_t> &dependencies)
//	    : dependents(dependents), dependencies(dependencies) {
//	}

// public:
//	catalog_entry_map_t<dependency_set_t> &dependents;
//	catalog_entry_map_t<catalog_entry_set_t> &dependencies;

// public:
//	optional_ptr<dependency_set_t> GetEntriesThatDependOnObject(CatalogEntry &object) {
//		auto entry = dependents.find(object);
//		if (entry == dependents.end()) {
//			return nullptr;
//		}
//		return &entry->second;
//	}
//	optional_ptr<catalog_entry_set_t> GetEntriesThatObjectDependsOn(CatalogEntry &object) {
//		auto entry = dependencies.find(object);
//		if (entry == dependencies.end()) {
//			return nullptr;
//		}
//		return &entry->second;
//	}
//	void AddForeignKeyConnection(CatalogEntry &entry, const string &fk_table);
//};

//! The DependencyManager is in charge of managing dependencies between catalog entries
class DependencyManager {
	friend class CatalogSet;
	friend class DependencySetCatalogEntry;

public:
	explicit DependencyManager(DuckCatalog &catalog);

	//! Scans all dependencies, returning pairs of (object, dependent)
	void Scan(ClientContext &context,
	          const std::function<void(CatalogEntry &, CatalogEntry &, DependencyType)> &callback);

	void AddOwnership(CatalogTransaction transaction, CatalogEntry &owner, CatalogEntry &entry);
	optional_ptr<DependencySetCatalogEntry> GetDependencySet(CatalogTransaction transaction,
	                                                         const string &mangled_name);
	DependencySetCatalogEntry &GetOrCreateDependencySet(CatalogTransaction transaction, const LogicalDependency &entry);

	//! Get the order of entries needed by EXPORT, the objects with no dependencies are exported first
	catalog_entry_vector_t GetExportOrder();

private:
	DuckCatalog &catalog;
	CatalogSet dependency_sets;

private:
	bool IsSystemEntry(CatalogEntry &entry) const;
	DependencySetCatalogEntry &GetOrCreateDependencySet(CatalogTransaction transaction, CatalogEntry &entry);
	void DropDependencySet(CatalogTransaction, CatalogEntry &entry);
	optional_ptr<DependencySetCatalogEntry> GetDependencySet(CatalogTransaction transaction, CatalogEntry &entry);

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, LogicalDependency dependency);
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, CatalogEntry &dependency);

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
};

} // namespace duckdb
