//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/dependency_catalog_set.hpp"
#include "duckdb/catalog/dependency.hpp"
#include <memory>

namespace duckdb {

class DependencyManager;
class DuckCatalog;
class DependencyCatalogEntry;

class DependencySetCatalogEntry {
public:
	DependencySetCatalogEntry(DuckCatalog &catalog, DependencyManager &dependency_manager, CatalogEntryInfo info);

public:
	DependencyCatalogSet &Dependencies();
	DependencyCatalogSet &Dependents();
	DependencyManager &Manager();

public:
	bool HasDependencyOn(CatalogTransaction transaction, CatalogEntry &entry);
	bool HasDependencyOn(CatalogTransaction transaction, DependencySetCatalogEntry &other);
	bool IsDependencyOf(CatalogTransaction transaction, CatalogEntry &entry);
	bool IsDependencyOf(CatalogTransaction transaction, DependencySetCatalogEntry &other);

private:
	bool HasDependencyOn(CatalogTransaction transaction, const MangledEntryName &mangled_name);
	bool IsDependencyOf(CatalogTransaction transaction, const MangledEntryName &mangled_name);

public:
	void PrintDependencies(CatalogTransaction transaction);
	void PrintDependents(CatalogTransaction transaction);

public:
	const MangledEntryName &MangledName() const;
	CatalogType EntryType() const;
	const string &EntrySchema() const;
	const string &EntryName() const;
	const CatalogEntryInfo &EntryInfo() const;

private:
	void VerifyDependencyName(const string &name);

private:
	DuckCatalog &catalog;
	MangledEntryName mangled_name;
	CatalogEntryInfo info;

	// These are proxies so we don't have a nested CatalogSet
	// Because the Catalog is not built to support this
	DependencyCatalogSet dependencies;
	DependencyCatalogSet dependents;
	DependencyManager &dependency_manager;
};

} // namespace duckdb
