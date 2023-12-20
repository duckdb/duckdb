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

	//! The indexed table information
	shared_ptr<DataTableInfo> info;

	//! We need the initial size of the index after the CREATE INDEX statement,
	//! as it is necessary to determine the auto checkpoint threshold
	idx_t initial_index_size;

public:
	string GetSchemaName() const override;
	string GetTableName() const override;

	//! Drops in-memory index data and marks all blocks on disk as free blocks, allowing to reclaim them
	void CommitDrop();
};

} // namespace duckdb
