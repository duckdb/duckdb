//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/dindex_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"

namespace duckdb {

//! An index catalog entry
class DIndexCatalogEntry : public IndexCatalogEntry {
public:
	//! Create an IndexCatalogEntry and initialize storage for it
	DIndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info);
	~DIndexCatalogEntry();

	shared_ptr<DataTableInfo> info;

public:
	string GetSchemaName() override;
	string GetTableName() override;
};

} // namespace duckdb
