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

class DependencyCatalogEntry : public InCatalogEntry {
public:
	DependencyCatalogEntry(Catalog &catalog, const CatalogEntryInfo &entry, const CatalogEntryInfo &from,
	                       DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR);
	~DependencyCatalogEntry() override;

public:
	const MangledEntryName &MangledName() const;
	CatalogType EntryType() const;
	const string &EntrySchema() const;
	const string &EntryName() const;
	const CatalogEntryInfo &EntryInfo() const;

	const MangledEntryName &FromMangledName() const;
	CatalogType FromType() const;
	const string &FromSchema() const;
	const string &FromName() const;
	const CatalogEntryInfo &FromInfo() const;

	DependencyType Type() const;

	// Create the corresponding dependency/dependent in the other set
	void CompleteLink(CatalogTransaction transaction, DependencyType type = DependencyType::DEPENDENCY_REGULAR);

private:
	const MangledEntryName mangled_name;
	const CatalogEntryInfo entry;

	const MangledEntryName from_mangled_name;
	const CatalogEntryInfo from;

	DependencyType dependency_type;
};

} // namespace duckdb
