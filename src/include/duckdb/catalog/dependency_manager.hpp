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

struct CatalogEntryInfo {
	CatalogType type;
	string schema;
	string name;
};

struct DependencyInfo {
	CatalogEntryInfo from;
	CatalogEntryInfo to;
	DependencyType from_type;
	DependencyType to_type;
};

struct MangledEntryName {
public:
	MangledEntryName(const CatalogEntryInfo &info);
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
	DependencySetCatalogEntry GetDependencySet(CatalogTransaction transaction, const CatalogEntryInfo &info);

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
	static MangledEntryName MangleName(const CatalogEntryInfo &info);
	static MangledEntryName MangleName(CatalogEntry &entry);
	static CatalogEntryInfo GetLookupProperties(CatalogEntry &entry);

private:
	void AddObject(CatalogTransaction transaction, CatalogEntry &object, const DependencyList &dependencies);
	void DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade);
	void AlterObject(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj);

private:
	void CreateDependency(CatalogTransaction transaction, const DependencyInfo &info);
	CatalogSet &Dependents();
	CatalogSet &Dependencies();
};

} // namespace duckdb
