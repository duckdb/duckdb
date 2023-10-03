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

//! A duck index entry
class DuckIndexEntry : public IndexCatalogEntry {
public:
	//! Create a DuckIndexEntry
	DuckIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info);
	~DuckIndexEntry() override;

	shared_ptr<DataTableInfo> info;

public:
	string GetSchemaName() const override;
	string GetTableName() const override;

	//! Drops in-memory index data and marks all blocks on disk as free blocks, allowing to reclaim them
	void CommitDrop();
};

} // namespace duckdb
