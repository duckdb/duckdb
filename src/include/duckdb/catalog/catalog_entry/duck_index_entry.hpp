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
class CommitDropState;
class IndexEntry;
class TableCatalogEntry;

//! The storage of a DuckIndexEntry, shared by all versions of the entry (e.g. after COMMENT ON INDEX)
struct IndexDataTableInfo {
	IndexDataTableInfo(shared_ptr<DataTableInfo> info_p, shared_ptr<IndexEntry> index_p);
	~IndexDataTableInfo();

	//! Pointer to the DataTableInfo
	shared_ptr<DataTableInfo> info;
	//! The index entry of the catalog entry
	shared_ptr<IndexEntry> index;
};

//! A duck index entry
class DuckIndexEntry : public IndexCatalogEntry {
public:
	//! Create a DuckIndexEntry
	DuckIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &create_info,
	               TableCatalogEntry &table);
	DuckIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &create_info,
	               shared_ptr<IndexDataTableInfo> storage_info);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;
	void Rollback(CatalogEntry &prev_entry) override;

	//! The indexed table information
	shared_ptr<IndexDataTableInfo> info;
	//! We need the initial size of the index after the CREATE INDEX statement,
	//! as it is necessary to determine the auto checkpoint threshold
	idx_t initial_index_size;

public:
	Identifier GetSchemaName() const override;
	Identifier GetTableName() const override;

	DataTableInfo &GetDataTableInfo() const;
	shared_ptr<IndexEntry> GetIndexEntry() const;

	//! Saves the removal of the index from the indexes of its table into drop_state, to be done after FlushCommit().
	void CommitDrop(CommitDropState &drop_state);
};

} // namespace duckdb
