//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/storage/data_pointer.hpp"

namespace duckdb {
class DatabaseInstance;
class ClientContext;
class MetaBlockReader;
class SchemaCatalogEntry;
class SequenceCatalogEntry;
class TableCatalogEntry;
class ViewCatalogEntry;

//! CheckpointManager is responsible for checkpointing the database
class CheckpointManager {
public:
	explicit CheckpointManager(DatabaseInstance &db);

	//! Checkpoint the current state of the WAL and flush it to the main storage. This should be called BEFORE any
	//! connction is available because right now the checkpointing cannot be done online. (TODO)
	void CreateCheckpoint();
	//! Load from a stored checkpoint
	void LoadFromStorage();

	//! The database
	DatabaseInstance &db;
	//! The metadata writer is responsible for writing schema information
	unique_ptr<MetaBlockWriter> metadata_writer;
	//! The table data writer is responsible for writing the DataPointers used by the table chunks
	unique_ptr<MetaBlockWriter> tabledata_writer;

private:
	void WriteSchema(SchemaCatalogEntry &schema);
	void WriteTable(TableCatalogEntry &table);
	void WriteView(ViewCatalogEntry &table);
	void WriteSequence(SequenceCatalogEntry &table);
	void WriteMacro(MacroCatalogEntry &table);

	void ReadSchema(ClientContext &context, MetaBlockReader &reader);
	void ReadTable(ClientContext &context, MetaBlockReader &reader);
	void ReadView(ClientContext &context, MetaBlockReader &reader);
	void ReadSequence(ClientContext &context, MetaBlockReader &reader);
	void ReadMacro(ClientContext &context, MetaBlockReader &reader);
};

} // namespace duckdb
