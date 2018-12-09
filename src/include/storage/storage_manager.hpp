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

constexpr const int64_t STORAGE_VERSION = 1;

constexpr const char *DATABASE_INFO_FILE = "meta.info";
constexpr const char *DATABASE_TEMP_INFO_FILE = "meta.info.tmp";
constexpr const char *STORAGE_FILES[] = {"data-a", "data-b"};
constexpr const char *WAL_FILES[] = {"duckdb-a.wal", "duckdb-b.wal"};
constexpr const char *SCHEMA_FILE = "schemas.csv";
constexpr const char *TABLE_LIST_FILE = "tables.csv";
constexpr const char *TABLE_FILE = "tableinfo.duck";

class Catalog;
class DuckDB;
class TransactionManager;

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
};

} // namespace duckdb
