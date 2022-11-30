//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

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

class CheckpointWriter {
public:
	explicit CheckpointWriter(AttachedDatabase &db) : db(db), catalog(Catalog::GetCatalog(db)) {
	}
	virtual ~CheckpointWriter() {
	}

	//! The database
	AttachedDatabase &db;
	//! The catalog
	Catalog &catalog;

	virtual MetaBlockWriter &GetMetaBlockWriter() = 0;
	virtual unique_ptr<TableDataWriter> GetTableDataWriter(TableCatalogEntry &table) = 0;

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
	CheckpointReader(Catalog &catalog) : catalog(catalog) {
	}
	virtual ~CheckpointReader() {
	}

protected:
	Catalog &catalog;

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

	virtual void ReadTableData(ClientContext &context, MetaBlockReader &reader, BoundCreateTableInfo &bound_info);
};

class SingleFileCheckpointReader final : public CheckpointReader {
public:
	explicit SingleFileCheckpointReader(SingleFileStorageManager &storage)
	    : CheckpointReader(Catalog::GetCatalog(storage.GetAttached())), storage(storage) {
	}

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
	SingleFileCheckpointWriter(AttachedDatabase &db, BlockManager &block_manager);

	//! Checkpoint the current state of the WAL and flush it to the main storage. This should be called BEFORE any
	//! connection is available because right now the checkpointing cannot be done online. (TODO)
	void CreateCheckpoint();

	virtual MetaBlockWriter &GetMetaBlockWriter() override;
	virtual unique_ptr<TableDataWriter> GetTableDataWriter(TableCatalogEntry &table) override;

	BlockManager &GetBlockManager();

private:
	//! The metadata writer is responsible for writing schema information
	unique_ptr<MetaBlockWriter> metadata_writer;
	//! The table data writer is responsible for writing the DataPointers used by the table chunks
	unique_ptr<MetaBlockWriter> table_metadata_writer;
	//! Because this is single-file storage, we can share partial blocks across
	//! an entire checkpoint.
	PartialBlockManager partial_block_manager;
};

} // namespace duckdb
