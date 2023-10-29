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
	                       CatalogType entry_type, const string &entry_schema, const string &entry_name,
	                       DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR);
	~DependencyCatalogEntry() override;

public:
	const string &MangledName() const;
	CatalogType EntryType() const;
	const string &EntrySchema() const;
	const string &EntryName() const;
	DependencyType Type() const;

	// Create the corresponding dependency/dependent in the other set
	void CompleteLink(CatalogTransaction transaction, DependencyType type = DependencyType::DEPENDENCY_REGULAR);

private:
	const string entry_name;
	const string schema;
	const CatalogType entry_type;
	DependencyType dependency_type;

	DependencyLinkSide side;
	DependencySetCatalogEntry &set;
};

} // namespace duckdb
