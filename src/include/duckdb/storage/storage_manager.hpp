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
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/storage/write_ahead_log.hpp"

namespace duckdb {
class BlockManager;
class Catalog;
class CheckpointWriter;
class DatabaseInstance;
class TransactionManager;
class TableCatalogEntry;

struct DatabaseSize {
	idx_t total_blocks = 0;
	idx_t block_size = 0;
	idx_t free_blocks = 0;
	idx_t used_blocks = 0;
	idx_t bytes = 0;
	idx_t wal_size = 0;
};

class StorageCommitState {
public:
	// Destruction of this object, without prior call to FlushCommit,
	// will roll back the committed changes.
	virtual ~StorageCommitState() {
	}

	// Make the commit persistent
	virtual void FlushCommit() = 0;
};

//! StorageManager is responsible for managing the physical storage of the
//! database on disk
class StorageManager {
public:
	StorageManager(DatabaseInstance &db, string path, bool read_only);
	virtual ~StorageManager();

	//! The database this storagemanager belongs to
	DatabaseInstance &db;

public:
	static StorageManager &GetStorageManager(ClientContext &context);
	static StorageManager &GetStorageManager(DatabaseInstance &db);

	//! Initialize a database or load an existing database from the given path
	void Initialize();

	DatabaseInstance &GetDatabase() {
		return db;
	}

	//! Get the WAL of the StorageManager, returns nullptr if in-memory
	WriteAheadLog *GetWriteAheadLog() {
		return wal.get();
	}

	string GetDBPath() {
		return path;
	}
	bool InMemory();

	virtual bool AutomaticCheckpoint(idx_t estimated_wal_bytes) = 0;
	virtual unique_ptr<StorageCommitState> GenStorageCommitState(Transaction &transaction, bool checkpoint) = 0;
	virtual bool IsCheckpointClean(block_id_t checkpoint_id) = 0;
	virtual void CreateCheckpoint(bool delete_wal = false, bool force_checkpoint = false) = 0;
	virtual DatabaseSize GetDatabaseSize() = 0;
	virtual shared_ptr<TableIOManager> GetTableIOManager(BoundCreateTableInfo *info) = 0;

protected:
	virtual void LoadDatabase() = 0;

	//! The path of the database
	string path;
	//! The WriteAheadLog of the storage manager
	unique_ptr<WriteAheadLog> wal;

	//! Whether or not the database is opened in read-only mode
	bool read_only;
};

//! Stores database in a single file.
class SingleFileStorageManager : public StorageManager {
public:
	SingleFileStorageManager(DatabaseInstance &db, string path, bool read_only);

	//! The BlockManager to read/store meta information and data in blocks
	unique_ptr<BlockManager> block_manager;
	//! TableIoManager
	unique_ptr<TableIOManager> table_io_manager;

public:
	bool AutomaticCheckpoint(idx_t estimated_wal_bytes) override;
	unique_ptr<StorageCommitState> GenStorageCommitState(Transaction &transaction, bool checkpoint) override;
	bool IsCheckpointClean(block_id_t checkpoint_id) override;
	void CreateCheckpoint(bool delete_wal, bool force_checkpoint) override;
	DatabaseSize GetDatabaseSize() override;
	shared_ptr<TableIOManager> GetTableIOManager(BoundCreateTableInfo *info) override;

protected:
	void LoadDatabase() override;
};
} // namespace duckdb
