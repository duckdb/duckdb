//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/duck_constraint_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/constraint_catalog_entry.hpp"

namespace duckdb {

//! A constraint catalog entry
class DuckConstraintEntry : public ConstraintCatalogEntry {
public:
	//! Create a ConstraintCatalogEntry and initialize storage for it
	DuckConstraintEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateConstraintInfo &info);
	~DuckConstraintEntry();

//	shared_ptr<DataTableInfo> info;

public:
	string GetSchemaName() const override;
	string GetTableName() const override;
	string GetConstraintName() const override;
};

} // namespace duckdb