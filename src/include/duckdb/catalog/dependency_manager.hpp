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
class DependencyEntry;
class DependencySetCatalogEntry;

struct CatalogEntryInfo {
	CatalogType type;
	string schema;
	string name;
};

// The subject of this dependency
struct DependencySubject {
	CatalogEntryInfo entry;
	//! The type of dependency this is (e.g, ownership)
	DependencySubjectFlags flags;
};

// The entry that relies on the other entry
struct DependencyDependent {
	CatalogEntryInfo entry;
	//! The type of dependency this is (e.g, blocking, non-blocking, ownership)
	DependencyDependentFlags flags;
};

//! Every dependency consists of a subject (the entry being depended on) and a dependent (the entry that has the
//! dependency)
struct DependencyInfo {
public:
	static DependencyInfo FromSubject(DependencyEntry &dep);
	static DependencyInfo FromDependent(DependencyEntry &dep);

public:
	DependencyDependent dependent;
	DependencySubject subject;
};

struct MangledEntryName {
public:
	MangledEntryName(const CatalogEntryInfo &info);
	MangledEntryName() = delete;

public:
	//! Format: Type\0Schema\0Name
	string name;
};

struct MangledDependencyName {
public:
	MangledDependencyName(const MangledEntryName &from, const MangledEntryName &to);
	MangledDependencyName() = delete;

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
	          const std::function<void(CatalogEntry &, CatalogEntry &, const DependencyDependentFlags &)> &callback);

	void AddOwnership(CatalogTransaction transaction, CatalogEntry &owner, CatalogEntry &entry);

private:
	DuckCatalog &catalog;
	CatalogSet subjects;
	CatalogSet dependents;

private:
	bool IsSystemEntry(CatalogEntry &entry) const;
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
	void RemoveDependency(CatalogTransaction transaction, const DependencyInfo &info);
	void CreateDependency(CatalogTransaction transaction, DependencyInfo &info);
	using dependency_entry_func_t = const std::function<unique_ptr<DependencyEntry>(
	    Catalog &catalog, const DependencyDependent &dependent, const DependencySubject &dependency)>;

	void CreateSubject(CatalogTransaction transaction, const DependencyInfo &info);
	void CreateDependent(CatalogTransaction transaction, const DependencyInfo &info);

	using dependency_callback_t = const std::function<void(DependencyEntry &)>;
	void ScanDependents(CatalogTransaction transaction, const CatalogEntryInfo &info, dependency_callback_t &callback);
	void ScanSubjects(CatalogTransaction transaction, const CatalogEntryInfo &info, dependency_callback_t &callback);
	void ScanSetInternal(CatalogTransaction transaction, const CatalogEntryInfo &info, bool subjects,
	                     dependency_callback_t &callback);
	void PrintSubjects(CatalogTransaction transaction, const CatalogEntryInfo &info);
	void PrintDependents(CatalogTransaction transaction, const CatalogEntryInfo &info);
	CatalogSet &Dependents();
	CatalogSet &Subjects();
};

} // namespace duckdb
