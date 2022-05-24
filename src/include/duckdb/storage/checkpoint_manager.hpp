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
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {
class DatabaseInstance;
class ClientContext;
class ColumnSegment;
class MetaBlockReader;
class SchemaCatalogEntry;
class SequenceCatalogEntry;
class TableCatalogEntry;
class ViewCatalogEntry;
class MatViewCatalogEntry;
class TypeCatalogEntry;

struct PartialColumnSegment {
	ColumnSegment *segment;
	uint32_t offset_in_block;
};

struct PartialBlock {
	block_id_t block_id;
	//! The block handle that stores this block
	shared_ptr<BlockHandle> block;
	//! The segments that are involved in the partial block
	vector<PartialColumnSegment> segments;

	void FlushToDisk(DatabaseInstance &db);
};

//! CheckpointManager is responsible for checkpointing the database
class CheckpointManager {
public:
	static constexpr const idx_t PARTIAL_BLOCK_THRESHOLD = Storage::BLOCK_SIZE / 5 * 4;

public:
	explicit CheckpointManager(DatabaseInstance &db);

	//! The database
	DatabaseInstance &db;
	//! The metadata writer is responsible for writing schema information
	unique_ptr<MetaBlockWriter> metadata_writer;
	//! The table data writer is responsible for writing the DataPointers used by the table chunks
	unique_ptr<MetaBlockWriter> tabledata_writer;

public:
	//! Checkpoint the current state of the WAL and flush it to the main storage. This should be called BEFORE any
	//! connction is available because right now the checkpointing cannot be done online. (TODO)
	void CreateCheckpoint();
	//! Load from a stored checkpoint
	void LoadFromStorage();

	//! Try to obtain a partially filled block that can fit "segment_size" bytes
	//! If successful, returns true and returns the block_id and offset_in_block to write to
	//! Otherwise, returns false
	bool GetPartialBlock(ColumnSegment *segment, idx_t segment_size, block_id_t &block_id, uint32_t &offset_in_block,
	                     PartialBlock *&partial_block_ptr, unique_ptr<PartialBlock> &owned_partial_block);

	//! Register a partially filled block that is filled with "segment_size" entries
	void RegisterPartialBlock(ColumnSegment *segment, idx_t segment_size, block_id_t block_id);

	//! Flush any remaining partial segments to disk
	void FlushPartialSegments();

private:
	void WriteSchema(SchemaCatalogEntry &schema);
	void WriteTable(TableCatalogEntry &table);
	void WriteView(ViewCatalogEntry &table);
	void WriteMatView(MatViewCatalogEntry &table);
	void WriteSequence(SequenceCatalogEntry &table);
	void WriteMacro(ScalarMacroCatalogEntry &table);
	void WriteTableMacro(TableMacroCatalogEntry &table);
	void WriteType(TypeCatalogEntry &table);

	void ReadSchema(ClientContext &context, MetaBlockReader &reader);
	void ReadTable(ClientContext &context, MetaBlockReader &reader);
	void ReadView(ClientContext &context, MetaBlockReader &reader);
	void ReadMatView(ClientContext &context, MetaBlockReader &reader);
	void ReadSequence(ClientContext &context, MetaBlockReader &reader);
	void ReadMacro(ClientContext &context, MetaBlockReader &reader);
	void ReadTableMacro(ClientContext &context, MetaBlockReader &reader);
	void ReadType(ClientContext &context, MetaBlockReader &reader);

private:
	//! A map of (available space -> PartialBlock) for partially filled blocks
	//! This is a multimap because there might be outstanding partial blocks with the same amount of left-over space
	multimap<idx_t, unique_ptr<PartialBlock>> partially_filled_blocks;
};

} // namespace duckdb
