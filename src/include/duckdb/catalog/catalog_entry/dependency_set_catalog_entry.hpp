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

class DependencySetCatalogEntry : public InCatalogEntry {
public:
	DependencySetCatalogEntry(Catalog &catalog, const string &name);
	~DependencySetCatalogEntry() override;

public:
	CatalogSet &Dependencies();
	CatalogSet &Dependents();
public:
	void AddDependency(CatalogTransaction transaction, CatalogEntry &dependency);
	void AddDependencies(CatalogTransaction transaction, DependencyList &dependencies);

	void AddDependent(CatalogTransaction transaction, CatalogEntry &dependent, DependencyType type);
	void AddDependents(CatalogTransaction transaction, DependencyList &dependents, DependencyType type);

	bool HasDependencyOn(CatalogTransaction transaction, CatalogEntry &entry);
private:
	string name;
	CatalogSet dependencies;
	CatalogSet dependents;
};


} // namespace duckdb
