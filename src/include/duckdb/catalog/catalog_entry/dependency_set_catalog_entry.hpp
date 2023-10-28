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
	DependencySetCatalogEntry(Catalog &catalog, DependencyManager &dependency_manager, const string &name);
	~DependencySetCatalogEntry() override;

public:
	CatalogSet &Dependencies();
	CatalogSet &Dependents();

public:
	void ScanDependents(CatalogTransaction transaction,
	                    const std::function<void(DependencyCatalogEntry &, DependencySetCatalogEntry &)> &callback);
	void ScanDependencies(CatalogTransaction transaction,
	                      const std::function<void(DependencyCatalogEntry &, DependencySetCatalogEntry &)> &callback);

public:
	// Add Dependencies
	void AddDependency(CatalogTransaction transaction, CatalogEntry &dependent,
	                   DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR);
	void AddDependency(CatalogTransaction transaction, Dependency dependent);
	void AddDependencies(CatalogTransaction transaction, DependencyList &dependencies);
	void AddDependencies(CatalogTransaction transaction, dependency_set_t &dependencies);

	// Add Dependents
	void AddDependent(CatalogTransaction transaction, CatalogEntry &dependent,
	                  DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR);
	void AddDependent(CatalogTransaction transaction, Dependency dependent);
	void AddDependents(CatalogTransaction transaction, DependencyList &dependents);
	void AddDependents(CatalogTransaction transaction, dependency_set_t &dependents);

public:
	void RemoveDependency(CatalogTransaction transaction, CatalogEntry &dependency);
	void RemoveDependent(CatalogTransaction transaction, CatalogEntry &dependent);

public:
	bool HasDependencyOn(CatalogEntry &entry, DependencyType type);
	bool IsDependencyOf(CatalogEntry &entry);

private:
	//! Skips the exemption for DEPENDENCY_OWNS, use 'HasDependencyOn' instead for that
	bool HasDependencyOnInternal(CatalogEntry &entry);

public:
	void PrintDependencies();
	void PrintDependents();

public:
	const string &MangledName() const;

private:
	string name;
	CatalogSet dependencies;
	CatalogSet dependents;
	DependencyManager &dependency_manager;
};

} // namespace duckdb
