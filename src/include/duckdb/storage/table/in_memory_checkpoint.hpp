//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/in_memory_checkpoint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/checkpoint/row_group_writer.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/checkpoint_manager.hpp"

namespace duckdb {

class InMemoryCheckpointer final : public CheckpointWriter {
public:
	InMemoryCheckpointer(QueryContext context, AttachedDatabase &db, BlockManager &block_manager,
	                     StorageManager &storage_manager, CheckpointType checkpoint_type);

	void CreateCheckpoint() override;

	MetadataWriter &GetMetadataWriter() override;
	MetadataManager &GetMetadataManager() override;
	unique_ptr<TableDataWriter> GetTableDataWriter(TableCatalogEntry &table) override;
	optional_ptr<ClientContext> GetClientContext() const {
		return context;
	}
	CheckpointType GetCheckpointType() const {
		return checkpoint_type;
	}
	PartialBlockManager &GetPartialBlockManager() {
		return partial_block_manager;
	}

public:
	void WriteTable(TableCatalogEntry &table, Serializer &serializer) override;

private:
	optional_ptr<ClientContext> context;
	PartialBlockManager partial_block_manager;
	StorageManager &storage_manager;
	CheckpointType checkpoint_type;
};

class InMemoryTableDataWriter : public TableDataWriter {
public:
	InMemoryTableDataWriter(InMemoryCheckpointer &checkpoint_manager, TableCatalogEntry &table);

public:
	void WriteUnchangedTable(MetaBlockPointer pointer, idx_t total_rows) override;
	void FinalizeTable(const TableStatistics &global_stats, DataTableInfo *info, Serializer &serializer) override;
	unique_ptr<RowGroupWriter> GetRowGroupWriter(RowGroup &row_group) override;
	CheckpointType GetCheckpointType() const override;
	MetadataManager &GetMetadataManager() override;

private:
	InMemoryCheckpointer &checkpoint_manager;
};

class InMemoryRowGroupWriter : public RowGroupWriter {
public:
	InMemoryRowGroupWriter(TableCatalogEntry &table, PartialBlockManager &partial_block_manager,
	                       InMemoryCheckpointer &checkpoint_manager);

public:
	CheckpointType GetCheckpointType() const override;
	WriteStream &GetPayloadWriter() override;
	MetaBlockPointer GetMetaBlockPointer() override;
	optional_ptr<MetadataManager> GetMetadataManager() override;

private:
	//! Underlying writer object
	InMemoryCheckpointer &checkpoint_manager;
	// Nop metadata writer
	MemoryStream metadata_writer;
};

struct InMemoryPartialBlock : public PartialBlock {
public:
	InMemoryPartialBlock(ColumnData &data, ColumnSegment &segment, PartialBlockState state,
	                     BlockManager &block_manager);
	~InMemoryPartialBlock() override;

public:
	void Flush(QueryContext context, const idx_t free_space_left) override;
	void Merge(PartialBlock &other, idx_t offset, idx_t other_size) override;
	void AddSegmentToTail(ColumnData &data, ColumnSegment &segment, uint32_t offset_in_block) override;
	void Clear() override;
};
} // namespace duckdb
