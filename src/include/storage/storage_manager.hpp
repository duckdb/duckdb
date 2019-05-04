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
#include "storage/write_ahead_log.hpp"

namespace duckdb {

class Catalog;
class DuckDB;
class TransactionManager;

//! StorageManager is responsible for managing the physical storage of the
//! database on disk
class StorageManager {
public:
	StorageManager(DuckDB &database, string path, bool read_only);
	//! Initialize a database or load an existing database from the given path
	void Initialize();
	//! Get the WAL of the StorageManager, returns nullptr if in-memory
	WriteAheadLog *GetWriteAheadLog() {
		return wal.IsInitialized() ? &wal : nullptr;
	}

	DuckDB &GetDatabase() {
		return database;
	}

private:
	//! Load the database from a directory
	void LoadDatabase();
	//! Load the initial database from the main storage (without WAL). Returns which alternate storage to write to.
	int LoadFromStorage();
	//! Checkpoint the current state of the WAL and flush it to the main storage. This should be called BEFORE any
	//! connction is available because right now the checkpointing cannot be done online. (TODO)
	void CreateCheckpoint(int iteration);
	//! Builds the data blocks for physical storage
	void BuildDataBlocks();

	//! The path of the database
	string path;
	//! The database this storagemanager belongs to
	DuckDB &database;
	//! The WriteAheadLog of the storage manager
	WriteAheadLog wal;
	//! Whether or not the database is opened in read-only mode
	bool read_only;
};

} // namespace duckdb
