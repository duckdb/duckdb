//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/table_data_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/storage/checkpoint/row_group_writer.hpp"

namespace duckdb {
class DuckTableEntry;
class TableStatistics;

//! The table data writer is responsible for writing the data of a table to storage.
//
//! This is meant to encapsulate and abstract:
//!  - Storage/encoding of table metadata (block pointers)
//!  - Mapping management of data block locations
//! Abstraction will support, for example: tiering, versioning, or splitting into multiple block managers.
class TableDataWriter {
public:
	explicit TableDataWriter(TableCatalogEntry &table, QueryContext context);
	virtual ~TableDataWriter();

public:
	void WriteTableData(Serializer &metadata_serializer);

	virtual void WriteUnchangedTable(MetaBlockPointer pointer, idx_t total_rows) = 0;
	virtual void FinalizeTable(const TableStatistics &global_stats, DataTableInfo &info, RowGroupCollection &collection,
	                           Serializer &serializer) = 0;
	virtual unique_ptr<RowGroupWriter> GetRowGroupWriter(RowGroup &row_group) = 0;

	virtual void AddRowGroup(RowGroupPointer &&row_group_pointer, unique_ptr<RowGroupWriter> writer);
	virtual CheckpointType GetCheckpointType() const = 0;
	virtual MetadataManager &GetMetadataManager() = 0;

	DatabaseInstance &GetDatabase();
	unique_ptr<TaskExecutor> CreateTaskExecutor();

protected:
	DuckTableEntry &table;
	optional_ptr<ClientContext> context;
	//! Pointers to the start of each row group.
	vector<RowGroupPointer> row_group_pointers;
};

class SingleFileTableDataWriter : public TableDataWriter {
public:
	SingleFileTableDataWriter(SingleFileCheckpointWriter &checkpoint_manager, TableCatalogEntry &table,
	                          MetadataWriter &table_data_writer);

public:
	void WriteUnchangedTable(MetaBlockPointer pointer, idx_t total_rows) override;
	void FinalizeTable(const TableStatistics &global_stats, DataTableInfo &info, RowGroupCollection &collection,
	                   Serializer &serializer) override;
	unique_ptr<RowGroupWriter> GetRowGroupWriter(RowGroup &row_group) override;
	CheckpointType GetCheckpointType() const override;
	MetadataManager &GetMetadataManager() override;

private:
	SingleFileCheckpointWriter &checkpoint_manager;
	//! Writes the actual table data
	MetadataWriter &table_data_writer;
	//! The root pointer, if we are re-using metadata of the table
	MetaBlockPointer existing_pointer;
	optional_idx existing_rows;
	vector<MetaBlockPointer> existing_pointers;
};

} // namespace duckdb
