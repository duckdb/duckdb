//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/checkpoint_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/chunk_collection.hpp"
#include "storage/storage_manager.hpp"
#include "storage/meta_block_writer.hpp"

namespace duckdb {
class SchemaCatalogEntry;
class TableCatalogEntry;

struct DataPointer {
	double min;
	double max;
	block_id_t block_id;
	uint32_t offset;
};

//! CheckpointManager is responsible for checkpointing the database
class CheckpointManager {
	//! Header size of individual Data Blocks
	static constexpr uint64_t DATA_BLOCK_HEADER_SIZE = sizeof(uint64_t) + sizeof(uint64_t);
public:
	CheckpointManager(StorageManager &manager);

	//! Checkpoint the current state of the WAL and flush it to the main storage. This should be called BEFORE any
	//! connction is available because right now the checkpointing cannot be done online. (TODO)
	void CreateCheckpoint();
private:
	void WriteSchema(Transaction &transaction, SchemaCatalogEntry *schema);
	void WriteTable(Transaction &transaction, TableCatalogEntry *table);
	void WriteTableData(Transaction &transaction, TableCatalogEntry *table);
	void WriteColumnData(TableCatalogEntry *table, ChunkCollection& collection, int32_t index, vector<DataPointer>& column_pointers);
private:
	//! The storage manager of the checkpoint manager
	StorageManager &manager;
	//! The block manager to write the checkpoint to
	BlockManager &block_manager;
	//! The database this storagemanager belongs to
	DuckDB &database;

	//! The metadata writer is responsible for writing schema information
	unique_ptr<MetaBlockWriter> metadata_writer;
	//! The table data writer is responsible for writing the DataPointers used by the table chunks
	unique_ptr<MetaBlockWriter> tabledata_writer;
};

}
