//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"

namespace duckdb {
class DatabaseInstance;
class ClientContext;
class ColumnSegment;
class MetaBlockReader;
class SchemaCatalogEntry;
class SequenceCatalogEntry;
class TableCatalogEntry;
class ViewCatalogEntry;
class TypeCatalogEntry;

struct PartialBlockState {
	block_id_t block_id;
	//! How big is the block we're writing to. (Total bytes to assign).
	uint32_t block_size;
	//! How many bytes of the allocation are used. (offset_in_block of next allocation)
	uint32_t offset_in_block;
	//! How many times has the block been used?
	uint32_t block_use_count;
};

struct PartialBlock {
	PartialBlockState state;

	explicit PartialBlock(PartialBlockState &&state) : state(move(state)) {}
	virtual ~PartialBlock() {}

	virtual void Flush() = 0;
};

struct PartialBlockAllocation {
	// BlockManager owning the block_id
	BlockManager *block_manager{nullptr};
	//! How many bytes assigned to the caller?
	uint32_t allocation_size;
	//! State of assigned block.
	PartialBlockState state;
	//! Arbitrary state related to partial block storage.
	unique_ptr<PartialBlock> partial_block;
};

//! Enables sharing blocks across some scope. Scope is whatever we want to share
//! blocks across. It may be an entire checkpoint or just a single row group.
//! In any case, they must share a block manager.
class PartialBlockManager {
public:
	// 20% free / 80% utilization
	static constexpr const idx_t DEFAULT_FREE_SPACE_THRESHOLD = Storage::BLOCK_SIZE / 5;
	// Max number of shared references to a block. No effective limit by default.
	static constexpr const idx_t DEFAULT_MAX_USE_COUNT = 1<<20;
	// No point letting map size grow unbounded. We'll drop blocks with the
	// least free space first.
	static constexpr const idx_t MAX_BLOCK_MAP_SIZE = 10;

	PartialBlockManager(BlockManager &block_manager,
			uint32_t free_space_threshold = DEFAULT_FREE_SPACE_THRESHOLD,
			uint32_t max_use_count = DEFAULT_MAX_USE_COUNT) :
		block_manager(block_manager), free_space_threshold(free_space_threshold),
		max_use_count(max_use_count) {}

	//! Flush any remaining partial blocks to disk
	void FlushPartialBlocks();

	PartialBlockAllocation GetBlockAllocation(uint32_t segment_size);

	virtual void AllocateBlock(PartialBlockState &state, uint32_t segment_size);

	//! Register a partially filled block that is filled with "segment_size" entries
	void RegisterPartialBlock(PartialBlockAllocation &&allocation);

protected:
	//! Try to obtain a partially filled block that can fit "segment_size" bytes
	//! If successful, returns true and returns the block_id and offset_in_block to write to
	//! Otherwise, returns false
	bool GetPartialBlock(idx_t segment_size, unique_ptr<PartialBlock> &state);

	BlockManager &block_manager;
	//! A map of (available space -> PartialBlock) for partially filled blocks
	//! This is a multimap because there might be outstanding partial blocks with
	//! the same amount of left-over space
	multimap<idx_t, unique_ptr<PartialBlock>> partially_filled_blocks;

	uint32_t free_space_threshold;
	uint32_t max_use_count;
};

class CheckpointWriter {
public:
	explicit CheckpointWriter(DatabaseInstance &db) : db(db) {}
	virtual ~CheckpointWriter() {}

	//! The database
	DatabaseInstance &db;

	virtual MetaBlockWriter &GetMetaBlockWriter() = 0;
	virtual unique_ptr<TableDataWriter> GetTableDataWriter(TableCatalogEntry &table) = 0;
	virtual BlockPointer WriteIndexData(IndexCatalogEntry &index_catalog) = 0;

protected:
	virtual void WriteSchema(SchemaCatalogEntry &schema);
	virtual void WriteTable(TableCatalogEntry &table);
	virtual void WriteView(ViewCatalogEntry &table);
	virtual void WriteSequence(SequenceCatalogEntry &table);
	virtual void WriteMacro(ScalarMacroCatalogEntry &table);
	virtual void WriteTableMacro(TableMacroCatalogEntry &table);
	virtual void WriteIndex(IndexCatalogEntry &index_catalog);
	virtual void WriteType(TypeCatalogEntry &table);
};

class CheckpointReader {
public:
	virtual ~CheckpointReader() {}

protected:
	virtual void LoadCheckpoint(ClientContext &context, MetaBlockReader &reader);
	virtual void ReadSchema(ClientContext &context, MetaBlockReader &reader);
	virtual void ReadTable(ClientContext &context, MetaBlockReader &reader);
	virtual void ReadView(ClientContext &context, MetaBlockReader &reader);
	virtual void ReadSequence(ClientContext &context, MetaBlockReader &reader);
	virtual void ReadMacro(ClientContext &context, MetaBlockReader &reader);
	virtual void ReadTableMacro(ClientContext &context, MetaBlockReader &reader);
	virtual void ReadIndex(ClientContext &context, MetaBlockReader &reader);
	virtual void ReadType(ClientContext &context, MetaBlockReader &reader);

	virtual void ReadTableData(ClientContext &context, MetaBlockReader &reader,
		BoundCreateTableInfo &bound_info);
};

class SingleFileCheckpointReader final : public CheckpointReader {
public:
	explicit SingleFileCheckpointReader(SingleFileStorageManager &storage) :
		storage(storage)
  {}

	void LoadFromStorage();

	//! The database
	SingleFileStorageManager &storage;
};

//! CheckpointWriter is responsible for checkpointing the database
class SingleFileRowGroupWriter;
class SingleFileTableDataWriter;

class SingleFileCheckpointWriter final : public CheckpointWriter {
	friend class SingleFileRowGroupWriter;
	friend class SingleFileTableDataWriter;
public:
	explicit SingleFileCheckpointWriter(DatabaseInstance &db, BlockManager &block_manager) :
		CheckpointWriter(db),
		partial_block_mgr(block_manager)
  {}

	//! Checkpoint the current state of the WAL and flush it to the main storage. This should be called BEFORE any
	//! connection is available because right now the checkpointing cannot be done online. (TODO)
	void CreateCheckpoint();

	virtual MetaBlockWriter &GetMetaBlockWriter() override;
	virtual unique_ptr<TableDataWriter> GetTableDataWriter(TableCatalogEntry &table) override;
	virtual BlockPointer WriteIndexData(IndexCatalogEntry &index_catalog) override;

	BlockManager &GetBlockManager();
private:
	//! The metadata writer is responsible for writing schema information
	unique_ptr<MetaBlockWriter> metadata_writer;
	//! The table data writer is responsible for writing the DataPointers used by the table chunks
	unique_ptr<MetaBlockWriter> table_metadata_writer;
	//! Because this is single-file storage, we can share partial blocks across
	//! an entire checkpoint.
	PartialBlockManager partial_block_mgr;
};

} // namespace duckdb
