//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension.hpp"

namespace duckdb {
class StorageManager;
class Catalog;
class TransactionManager;
class ConnectionManager;
class FileSystem;

enum class AccessMode : uint8_t { UNDEFINED = 0, AUTOMATIC = 1, READ_ONLY = 2, READ_WRITE = 3 };

// this is optional and only used in tests at the moment
struct DBConfig {
	friend class DuckDB;
	friend class StorageManager;

public:
	~DBConfig();

	//! Access mode of the database (AUTOMATIC, READ_ONLY or READ_WRITE)
	AccessMode access_mode = AccessMode::AUTOMATIC;
	// Checkpoint when WAL reaches this size
	idx_t checkpoint_wal_size = 1 << 20;
	//! Whether or not to use Direct IO, bypassing operating system buffers
	bool use_direct_io = false;
	//! The FileSystem to use, can be overwritten to allow for injecting custom file systems for testing purposes (e.g.
	//! RamFS or something similar)
	unique_ptr<FileSystem> file_system;
	//! The maximum memory used by the database system (in bytes). Default: Infinite
	idx_t maximum_memory = (idx_t)-1;
	//! Whether or not to create and use a temporary directory to store intermediates that do not fit in memory
	bool use_temporary_directory = true;
	//! Directory to store temporary structures that do not fit in memory
	string temporary_directory;
	//! The collation type of the database
	string collation = string();

private:
	// FIXME: don't set this as a user: used internally (only for now)
	bool checkpoint_only = false;
};

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class Connection;
class DuckDB {
public:
	DuckDB(const char *path = nullptr, DBConfig *config = nullptr);
	DuckDB(const string &path, DBConfig *config = nullptr);

	~DuckDB();

	unique_ptr<FileSystem> file_system;
	unique_ptr<StorageManager> storage;
	unique_ptr<Catalog> catalog;
	unique_ptr<TransactionManager> transaction_manager;
	unique_ptr<ConnectionManager> connection_manager;

	AccessMode access_mode;
	bool use_direct_io;
	bool checkpoint_only;
	idx_t checkpoint_wal_size;
	idx_t maximum_memory;
	string temporary_directory;
	string collation;

public:
	template <class T> void LoadExtension() {
		T extension;
		extension.Load(*this);
	}

private:
	void Configure(DBConfig &config);
};

} // namespace duckdb
