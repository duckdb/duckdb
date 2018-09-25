//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// storage/storage_manager.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

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
	StorageManager(DuckDB &database, std::string path);
	//! Initialize a database or load an existing database from the given path
	void Initialize();
	//! Get the WAL of the StorageManager, returns nullptr if in-memory
	WriteAheadLog *GetWriteAheadLog() {
		return wal.IsInitialized() ? &wal : nullptr;
	}

  private:
	void LoadDatabase(std::string &path);

	//! The path of the database
	std::string path;
	//! The database this storagemanager belongs to
	DuckDB &database;
	//! The WriteAheadLog of the storage manager
	WriteAheadLog wal;
};

} // namespace duckdb
