//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/table_data_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/checkpoint/row_group_writer.hpp"

namespace duckdb {

//! The table data writer is responsible for writing the data of a table to
//! storage.
//
//! This is meant to encapsulate and abstract:
//!  - Storage/encoding of table metadata (block pointers)
//!  - Mapping management of data block locations
//! Abstraction will support, for example: tiering, versioning, or splitting into multiple block managers.
class TableDataWriter {
public:
	explicit TableDataWriter(TableCatalogEntry &table);
	virtual ~TableDataWriter();

public:
	void WriteTableData();

	CompressionType GetColumnCompressionType(idx_t i);

	virtual void FinalizeTable(vector<unique_ptr<BaseStatistics>> &&global_stats, DataTableInfo *info) = 0;
	virtual unique_ptr<RowGroupWriter> GetRowGroupWriter(RowGroup &row_group) = 0;

	virtual void AddRowGroup(RowGroupPointer &&row_group_pointer, unique_ptr<RowGroupWriter> &&writer);

protected:
	TableCatalogEntry &table;
	// Pointers to the start of each row group.
	vector<RowGroupPointer> row_group_pointers;
};

class SingleFileTableDataWriter : public TableDataWriter {
public:
	SingleFileTableDataWriter(SingleFileCheckpointWriter &checkpoint_manager, TableCatalogEntry &table,
	                          MetaBlockWriter &table_data_writer, MetaBlockWriter &meta_data_writer);

public:
	virtual void FinalizeTable(vector<unique_ptr<BaseStatistics>> &&global_stats, DataTableInfo *info) override;
	virtual unique_ptr<RowGroupWriter> GetRowGroupWriter(RowGroup &row_group) override;

private:
	SingleFileCheckpointWriter &checkpoint_manager;
	// Writes the actual table data
	MetaBlockWriter &table_data_writer;
	// Writes the metadata of the table
	MetaBlockWriter &meta_data_writer;
};

} // namespace duckdb
