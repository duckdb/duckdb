//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/storage_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/helper.hpp"
#include "storage/data_table.hpp"
#include "storage/directory_block_manager.hpp"
#include "storage/meta_block_writer.hpp"
#include "storage/write_ahead_log.hpp"

namespace duckdb {

class Catalog;
class ChunkCollection;
class DuckDB;
class TransactionManager;
class TableCatalogEntry;

struct DataPointer {
	double min;
	double max;
	block_id_t block_id;
	uint32_t offset;
};

constexpr const size_t CHUNK_THRESHOLD = 1024000; //! 1000 data chunks

//! StorageManager is responsible for managing the physical storage of the
//! database on disk
class StorageManager {
public:
	StorageManager(DuckDB &database, string path);
	//! Initialize a database or load an existing database from the given path
	void Initialize();
	//! Get the WAL of the StorageManager, returns nullptr if in-memory
	WriteAheadLog *GetWriteAheadLog() {
		return wal.IsInitialized() ? &wal : nullptr;
	}

	DuckDB &GetDatabase() {
		return database;
	}
	//! The BlockManager to read/store meta information and data in blocks
	unique_ptr<BlockManager> block_manager;

private:
	//! Load the database from a directory
	void LoadDatabase();
	//! Load the initial database from the main storage (without WAL). Returns which alternate storage to write to.
	void LoadFromStorage();
	//! Checkpoint the current state of the WAL and flush it to the main storage. This should be called BEFORE any
	//! connction is available because right now the checkpointing cannot be done online. (TODO)
	void CreateCheckpoint();


	void WriteSchema(Transaction &transaction, SchemaCatalogEntry *schema);
	void WriteTable(Transaction &transaction, TableCatalogEntry *table);
	void WriteTableData(Transaction &transaction, TableCatalogEntry *table);
	void WriteColumnData(TableCatalogEntry *table, ChunkCollection& collection, int32_t index, vector<DataPointer>& column_pointers);

	//! The path of the database
	string path;
	//! The database this storagemanager belongs to
	DuckDB &database;
	//! The WriteAheadLog of the storage manager
	WriteAheadLog wal;

	unique_ptr<MetaBlockWriter> metadata_writer;
	unique_ptr<MetaBlockWriter> tabledata_writer;

};

} // namespace duckdb
