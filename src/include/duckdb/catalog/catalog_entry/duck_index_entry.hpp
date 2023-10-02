//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/duck_index_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"

namespace duckdb {

//! An index catalog entry
class DuckIndexEntry : public IndexCatalogEntry {
public:
	//! Create an IndexCatalogEntry and initialize storage for it
	DuckIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info);
	~DuckIndexEntry();

	shared_ptr<DataTableInfo> info;

public:
	string GetSchemaName() const override;
	string GetTableName() const override;
	//! This drops in-memory index data and marks all blocks on disk as free blocks, allowing to reclaim them
	void CommitDrop();
};

} // namespace duckdb
