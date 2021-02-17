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
class DatabaseInstance;
class TransactionManager;
class TableCatalogEntry;

//! StorageManager is responsible for managing the physical storage of the
//! database on disk
class StorageManager {
public:
	StorageManager(DatabaseInstance &db, string path, bool read_only);
	~StorageManager();

	//! The BlockManager to read/store meta information and data in blocks
	unique_ptr<BlockManager> block_manager;
	//! The BufferManager of the database
	unique_ptr<BufferManager> buffer_manager;
	//! The database this storagemanager belongs to
	DatabaseInstance &db;

public:
	static StorageManager &GetStorageManager(ClientContext &context);
	static StorageManager &GetStorageManager(DatabaseInstance &db);

	//! Initialize a database or load an existing database from the given path
	void Initialize();
	//! Get the WAL of the StorageManager, returns nullptr if in-memory
	WriteAheadLog *GetWriteAheadLog() {
		return wal.initialized ? &wal : nullptr;
	}

	DatabaseInstance &GetDatabase() {
		return db;
	}

	void CreateCheckpoint(bool delete_wal = false, bool force_checkpoint = false);

	string GetDBPath() {
		return path;
	}
	bool InMemory();

private:
	//! Load the database from a directory
	void LoadDatabase();

	//! The path of the database
	string path;
	//! The WriteAheadLog of the storage manager
	WriteAheadLog wal;

	//! Whether or not the database is opened in read-only mode
	bool read_only;
};

} // namespace duckdb
