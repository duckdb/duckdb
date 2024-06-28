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
class MetadataReader;
class SchemaCatalogEntry;
class SequenceCatalogEntry;
class TableCatalogEntry;
class ViewCatalogEntry;
class TypeCatalogEntry;

class CheckpointWriter {
public:
	explicit CheckpointWriter(AttachedDatabase &db) : db(db) {
	}
	virtual ~CheckpointWriter() {
	}

	//! The database
	AttachedDatabase &db;

	virtual MetadataManager &GetMetadataManager() = 0;
	virtual MetadataWriter &GetMetadataWriter() = 0;
	virtual unique_ptr<TableDataWriter> GetTableDataWriter(TableCatalogEntry &table) = 0;

protected:
	virtual void WriteEntry(CatalogEntry &entry, Serializer &serializer);
	virtual void WriteSchema(SchemaCatalogEntry &schema, Serializer &serializer);
	virtual void WriteTable(TableCatalogEntry &table, Serializer &serializer) = 0;
	virtual void WriteView(ViewCatalogEntry &table, Serializer &serializer);
	virtual void WriteSequence(SequenceCatalogEntry &table, Serializer &serializer);
	virtual void WriteMacro(ScalarMacroCatalogEntry &table, Serializer &serializer);
	virtual void WriteTableMacro(TableMacroCatalogEntry &table, Serializer &serializer);
	virtual void WriteIndex(IndexCatalogEntry &index_catalog_entry, Serializer &serializer);
	virtual void WriteType(TypeCatalogEntry &type, Serializer &serializer);
};

class CheckpointReader {
public:
	explicit CheckpointReader(Catalog &catalog) : catalog(catalog) {
	}
	virtual ~CheckpointReader() {
	}

protected:
	Catalog &catalog;

protected:
	virtual void LoadCheckpoint(CatalogTransaction transaction, MetadataReader &reader);
	virtual void ReadEntry(CatalogTransaction transaction, Deserializer &deserializer);
	virtual void ReadSchema(CatalogTransaction transaction, Deserializer &deserializer);
	virtual void ReadTable(CatalogTransaction transaction, Deserializer &deserializer);
	virtual void ReadView(CatalogTransaction transaction, Deserializer &deserializer);
	virtual void ReadSequence(CatalogTransaction transaction, Deserializer &deserializer);
	virtual void ReadMacro(CatalogTransaction transaction, Deserializer &deserializer);
	virtual void ReadTableMacro(CatalogTransaction transaction, Deserializer &deserializer);
	virtual void ReadIndex(CatalogTransaction transaction, Deserializer &deserializer);
	virtual void ReadType(CatalogTransaction transaction, Deserializer &deserializer);

	virtual void ReadTableData(CatalogTransaction transaction, Deserializer &deserializer,
	                           BoundCreateTableInfo &bound_info);
};

class SingleFileCheckpointReader final : public CheckpointReader {
public:
	explicit SingleFileCheckpointReader(SingleFileStorageManager &storage)
	    : CheckpointReader(Catalog::GetCatalog(storage.GetAttached())), storage(storage) {
	}

	void LoadFromStorage();
	MetadataManager &GetMetadataManager();

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
	SingleFileCheckpointWriter(AttachedDatabase &db, BlockManager &block_manager, CheckpointType checkpoint_type);

	//! Checkpoint the current state of the WAL and flush it to the main storage. This should be called BEFORE any
	//! connection is available because right now the checkpointing cannot be done online. (TODO)
	void CreateCheckpoint();

	MetadataWriter &GetMetadataWriter() override;
	MetadataManager &GetMetadataManager() override;
	unique_ptr<TableDataWriter> GetTableDataWriter(TableCatalogEntry &table) override;

	BlockManager &GetBlockManager();
	CheckpointType GetCheckpointType() const {
		return checkpoint_type;
	}

public:
	void WriteTable(TableCatalogEntry &table, Serializer &serializer) override;

private:
	//! The metadata writer is responsible for writing schema information
	unique_ptr<MetadataWriter> metadata_writer;
	//! The table data writer is responsible for writing the DataPointers used by the table chunks
	unique_ptr<MetadataWriter> table_metadata_writer;
	//! Because this is single-file storage, we can share partial blocks across
	//! an entire checkpoint.
	PartialBlockManager partial_block_manager;
	//! Checkpoint type
	CheckpointType checkpoint_type;
};

} // namespace duckdb
