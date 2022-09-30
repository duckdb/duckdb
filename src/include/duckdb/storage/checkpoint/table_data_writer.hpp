//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/table_data_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/checkpoint_manager.hpp"

namespace duckdb {
class CheckpointWriter;
class ColumnCheckpointState;
class ColumnData;
class ColumnSegment;
class RowGroup;
class BaseStatistics;
class SegmentStatistics;

// Writes data for an entire row group.
class RowGroupWriter {
public:
	RowGroupWriter(TableCatalogEntry &table, PartialBlockManager &partial_block_mgr) : table(table), partial_block_mgr(partial_block_mgr) {}

	CompressionType GetColumnCompressionType(idx_t i);

	virtual void WriteColumnDataPointers(ColumnCheckpointState &column_checkpoint_state) = 0;

	virtual MetaBlockWriter &GetPayloadWriter() = 0;

	void RegisterPartialBlock(PartialBlockAllocation &&allocation);
	PartialBlockAllocation GetBlockAllocation(uint32_t segment_size);

protected:
	TableCatalogEntry &table;
	PartialBlockManager &partial_block_mgr;
};

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

// Writes data for an entire row group.
class SingleFileRowGroupWriter : public RowGroupWriter {
public:
	SingleFileRowGroupWriter(TableCatalogEntry &table, PartialBlockManager &partial_block_mgr, MetaBlockWriter &table_data_writer) :
		RowGroupWriter(table, partial_block_mgr), table_data_writer(table_data_writer) {}

	virtual void WriteColumnDataPointers(ColumnCheckpointState &column_checkpoint_state) override;

	virtual MetaBlockWriter &GetPayloadWriter() override;

	//! MetaBlockWriter is a cursor on a given BlockManager. This returns the
	//! cursor against which we should write payload data for the specified RowGroup.
	MetaBlockWriter &table_data_writer;
};

class SingleFileTableDataWriter : public TableDataWriter {
public:
	SingleFileTableDataWriter(SingleFileCheckpointWriter &checkpoint_manager, TableCatalogEntry &table,
	                MetaBlockWriter &table_data_writer, MetaBlockWriter &meta_data_writer);

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
