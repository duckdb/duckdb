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
	DependencySetCatalogEntry(Catalog &catalog, DependencyManager &dependency_manager, CatalogEntry &object);
	~DependencySetCatalogEntry() override;

public:
	CatalogSet &Dependencies();
	CatalogSet &Dependents();

public:
	using dependency_callback_t = const std::function<void(DependencyCatalogEntry &)>;
	void ScanDependents(CatalogTransaction transaction, dependency_callback_t &callback);
	void ScanDependencies(CatalogTransaction transaction, dependency_callback_t &callback);

public:
	// Add Dependencies
	void AddDependency(CatalogTransaction transaction, CatalogEntry &dependent,
	                   DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR);
	void AddDependency(CatalogTransaction transaction, Dependency dependent);
	void AddDependencies(CatalogTransaction transaction, const DependencyList &dependencies);
	void AddDependencies(CatalogTransaction transaction, const dependency_set_t &dependencies);

	// Add Dependents
	void AddDependent(CatalogTransaction transaction, CatalogEntry &dependent,
	                  DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR);
	void AddDependent(CatalogTransaction transaction, const Dependency dependent);
	void AddDependents(CatalogTransaction transaction, const DependencyList &dependents);
	void AddDependents(CatalogTransaction transaction, const dependency_set_t &dependents);

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
