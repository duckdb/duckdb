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
#include "duckdb/catalog/dependency.hpp"
#include <memory>

namespace duckdb {

class DependencyManager;
class DependencyCatalogEntry;

class DependencySetCatalogEntry : public InCatalogEntry {
public:
	DependencySetCatalogEntry(Catalog &catalog, DependencyManager &dependency_manager, CatalogType entry_type,
	                          const string &entry_schema, const string &entry_name);
	~DependencySetCatalogEntry() override;

public:
	CatalogSet &Dependencies();
	CatalogSet &Dependents();
	DependencyManager &Manager();

public:
	using dependency_callback_t = const std::function<void(DependencyCatalogEntry &)>;
	void ScanDependents(CatalogTransaction transaction, dependency_callback_t &callback);
	void ScanDependencies(CatalogTransaction transaction, dependency_callback_t &callback);

public:
	// Add Dependencies
	DependencyCatalogEntry &AddDependency(CatalogTransaction transaction, CatalogEntry &dependent,
	                                      DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR);
	DependencyCatalogEntry &AddDependency(CatalogTransaction transaction, Dependency dependent);
	void AddDependencies(CatalogTransaction transaction, const DependencyList &dependencies);
	void AddDependencies(CatalogTransaction transaction, const dependency_set_t &dependencies);

	// Add Dependents
	DependencyCatalogEntry &AddDependent(CatalogTransaction transaction, CatalogEntry &dependent,
	                                     DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR);
	DependencyCatalogEntry &AddDependent(CatalogTransaction transaction, const Dependency dependent);
	void AddDependents(CatalogTransaction transaction, const DependencyList &dependents);
	void AddDependents(CatalogTransaction transaction, const dependency_set_t &dependents);

	// Get dependent/dependency
	DependencyCatalogEntry &GetDependency(CatalogTransaction &transaction, CatalogEntry &object);
	DependencyCatalogEntry &GetDependent(CatalogTransaction &transaction, CatalogEntry &object);

public:
	void RemoveDependency(CatalogTransaction transaction, CatalogEntry &dependency);
	void RemoveDependent(CatalogTransaction transaction, CatalogEntry &dependent);

public:
	bool HasDependencyOn(CatalogTransaction transaction, CatalogEntry &entry, DependencyType type);
	bool IsDependencyOf(CatalogTransaction transaction, CatalogEntry &entry);

private:
	//! Skips the exemption for DEPENDENCY_OWNS, use 'HasDependencyOn' instead for that
	void ScanSetInternal(CatalogTransaction transaction, bool dependencies, dependency_callback_t &callback);

public:
	void PrintDependencies(CatalogTransaction transaction);
	void PrintDependents(CatalogTransaction transaction);

public:
	const string &MangledName() const;
	CatalogType EntryType() const;
	const string &EntrySchema() const;
	const string &EntryName() const;

private:
	const string entry_name;
	const string schema;
	const CatalogType entry_type;

	CatalogSet dependencies;
	CatalogSet dependents;
	DependencyManager &dependency_manager;
};

} // namespace duckdb
