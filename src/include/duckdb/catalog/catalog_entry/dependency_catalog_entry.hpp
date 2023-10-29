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

//! Resembles a connection between an object and the CatalogEntry that can be retrieved from the Catalog using the
//! identifiers listed here

class DependencyCatalogEntry : public InCatalogEntry {
public:
	DependencyCatalogEntry(Catalog &catalog, CatalogEntry &entry,
	                       DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR);
	~DependencyCatalogEntry() override;

public:
	const string &MangledName() const;
	CatalogType EntryType() const;
	const string &EntrySchema() const;
	const string &EntryName() const;
	DependencyType Type() const;

private:
	const string entry_name;
	const string schema;
	const CatalogType entry_type;
	DependencyType dependency_type;
};

} // namespace duckdb
