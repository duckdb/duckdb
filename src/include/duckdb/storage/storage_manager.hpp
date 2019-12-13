//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/storage_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/write_ahead_log.hpp"

namespace duckdb {
class BlockManager;
class Catalog;
class DuckDB;
class TransactionManager;
class TableCatalogEntry;

//! StorageManager is responsible for managing the physical storage of the
//! database on disk
class StorageManager {
public:
	StorageManager(DuckDB &database, string path, bool read_only);
	~StorageManager();

	//! Initialize a database or load an existing database from the given path
	void Initialize();
	//! Get the WAL of the StorageManager, returns nullptr if in-memory
	WriteAheadLog *GetWriteAheadLog() {
		return wal.initialized ? &wal : nullptr;
	}

	DuckDB &GetDatabase() {
		return database;
	}
	//! The BlockManager to read/store meta information and data in blocks
	unique_ptr<BlockManager> block_manager;
	//! The BufferManager of the database
	unique_ptr<BufferManager> buffer_manager;
	//! The database this storagemanager belongs to
	DuckDB &database;

private:
	//! Load the database from a directory
	void LoadDatabase();
	//! Create a checkpoint of the database
	void Checkpoint(string wal_path);

	//! The path of the database
	string path;
	//! The WriteAheadLog of the storage manager
	WriteAheadLog wal;

	//! Whether or not the database is opened in read-only mode
	bool read_only;
};

} // namespace duckdb
