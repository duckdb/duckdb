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

class DependencySetCatalogEntry;

//! Resembles a connection between an object and the CatalogEntry that can be retrieved from the Catalog using the
//! identifiers listed here

enum class DependencyLinkSide { DEPENDENCY, DEPENDENT };

class DependencyCatalogEntry : public InCatalogEntry {
public:
	DependencyCatalogEntry(DependencyLinkSide side, Catalog &catalog, DependencySetCatalogEntry &set,
	                       const LogicalDependency &internal, DependencyFlags flags = DependencyFlags());
	~DependencyCatalogEntry() override;

public:
	const string &MangledName() const;
	CatalogType EntryType() const;
	const string &EntrySchema() const;
	const string &EntryName() const;
	const LogicalDependency &Internal() const;
	const DependencyFlags &Flags() const;

	// Create the corresponding dependency/dependent in the other set
	void CompleteLink(CatalogTransaction transaction, DependencyFlags flags = DependencyFlags());
	DependencyCatalogEntry &GetLink(optional_ptr<CatalogTransaction> transaction);

private:
	LogicalDependency internal;
	const DependencyFlags flags;

	DependencyLinkSide side;
	DependencySetCatalogEntry &set;
};

} // namespace duckdb
