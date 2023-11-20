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
class DependencyList;
class DependencyCatalogEntry;
class DependencySetCatalogEntry;

struct MangledEntryName {
public:
	MangledEntryName(CatalogType type, const string &schema, const string &name);
	// TODO: delete me later
	MangledEntryName() {
	}

public:
	//! Format: Type\0Schema\0Name
	string name;
};

struct MangledDependencyName {
public:
	MangledDependencyName(const MangledEntryName &from, const MangledEntryName &to);

public:
	//! Format: MangledEntryName\0MangledEntryName
	string name;
};

struct CatalogEntryInfo {
	CatalogType type;
	string schema;
	string name;
};

struct DependencyInfo {
	CatalogEntryInfo from;
	CatalogEntryInfo to;
};

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
	DependencySetCatalogEntry GetDependencySet(CatalogTransaction transaction, CatalogType type, const string &schema,
	                                           const string &name);

private:
	DuckCatalog &catalog;
	CatalogSet dependencies;
	CatalogSet dependents;

private:
	bool IsSystemEntry(CatalogEntry &entry) const;
	DependencySetCatalogEntry GetDependencySet(CatalogTransaction transaction, CatalogEntry &entry);

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, CatalogEntry &dependency);

	void CleanupDependencies(CatalogTransaction transaction, CatalogEntry &entry);

public:
	static string GetSchema(CatalogEntry &entry);
	static MangledEntryName MangleName(CatalogType type, const string &schema, const string &name);
	static MangledEntryName MangleName(CatalogEntry &entry);
	static void GetLookupProperties(CatalogEntry &entry, string &schema, string &name, CatalogType &type);

private:
	void AddObject(CatalogTransaction transaction, CatalogEntry &object, const DependencyList &dependencies);
	void DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade);
	void AlterObject(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj);
};

} // namespace duckdb
