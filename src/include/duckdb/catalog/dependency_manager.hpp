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
#include "duckdb/common/string_util.hpp"

#include <functional>

namespace duckdb {
class DuckCatalog;
class ClientContext;
class DependencyList;
class DependencyEntry;
class LogicalDependencyList;

// The subject of this dependency
struct DependencySubject {
	CatalogEntryInfo entry;
	DependencyFlags flags;
};

// The entry that relies on the other entry
struct DependencyReliant {
	CatalogEntryInfo entry;
	DependencyFlags flags;
};

struct DependencyInfo {
public:
	static DependencyInfo FromDependency(DependencyEntry &dep);
	static DependencyInfo FromDependent(DependencyEntry &dep);

public:
	DependencyReliant dependent;
	DependencySubject dependency;
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

public:
	bool operator==(const MangledEntryName &other) const {
		return StringUtil::CIEquals(other.name, name);
	}
	bool operator!=(const MangledEntryName &other) const {
		return !(*this == other);
	}
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

public:
	explicit DependencyManager(DuckCatalog &catalog);

	//! Scans all dependencies, returning pairs of (object, dependent)
	void Scan(ClientContext &context,
	          const std::function<void(CatalogEntry &, CatalogEntry &, DependencyFlags)> &callback);

	void AddOwnership(CatalogTransaction transaction, CatalogEntry &owner, CatalogEntry &entry);

	//! Get the order of entries needed by EXPORT, the objects with no dependencies are exported first
	catalog_entry_vector_t GetExportOrder(optional_ptr<CatalogTransaction> transaction = nullptr);

	catalog_entry_vector_t GetDependencySets(optional_ptr<CatalogTransaction> transaction);

private:
	DuckCatalog &catalog;
	CatalogSet dependencies;
	CatalogSet dependents;

private:
	bool IsSystemEntry(CatalogEntry &entry) const;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const LogicalDependency &dependency);
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, CatalogEntry &dependency);

	void CleanupDependencies(CatalogTransaction transaction, CatalogEntry &entry);

public:
	static string GetSchema(const CatalogEntry &entry);
	static MangledEntryName MangleName(const CatalogEntryInfo &info);
	static MangledEntryName MangleName(const CatalogEntry &entry);
	static CatalogEntryInfo GetLookupProperties(const CatalogEntry &entry);

private:
	void AddObject(CatalogTransaction transaction, CatalogEntry &object, const LogicalDependencyList &dependencies);
	void DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade);
	void AlterObject(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj,
	                 const LogicalDependencyList &dependencies);

private:
	void RemoveDependency(CatalogTransaction transaction, const DependencyInfo &info);
	void CreateDependency(CatalogTransaction transaction, const DependencyInfo &info);
	void CreateDependencies(CatalogTransaction transaction, const CatalogEntry &object,
	                        const LogicalDependencyList dependencies);
	using dependency_entry_func_t = const std::function<unique_ptr<DependencyEntry>(
	    Catalog &catalog, const DependencyReliant &dependent, const DependencySubject &dependency)>;

	void CreateDependencyInternal(CatalogTransaction transaction, const DependencyInfo &info, bool dependency);

	using dependency_callback_t = const std::function<void(DependencyEntry &)>;
	void ScanDependents(CatalogTransaction transaction, const CatalogEntryInfo &info, dependency_callback_t &callback);
	void ScanDependencies(CatalogTransaction transaction, const CatalogEntryInfo &info,
	                      dependency_callback_t &callback);
	void ScanSetInternal(CatalogTransaction transaction, const CatalogEntryInfo &info, bool dependencies,
	                     dependency_callback_t &callback);
	void PrintDependencies(CatalogTransaction transaction, const CatalogEntryInfo &info);
	void PrintDependents(CatalogTransaction transaction, const CatalogEntryInfo &info);
	CatalogSet &Dependents();
	CatalogSet &Dependencies();
};

} // namespace duckdb
