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
#include "duckdb/storage/database_size.hpp"

namespace duckdb {
class BlockManager;
class Catalog;
class CheckpointWriter;
class DatabaseInstance;
class TransactionManager;
class TableCatalogEntry;

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
	StorageManager(AttachedDatabase &db, string path, bool read_only);
	virtual ~StorageManager();

public:
	static StorageManager &Get(AttachedDatabase &db);
	static StorageManager &Get(Catalog &catalog);

	//! Initialize a database or load an existing database from the given path
	void Initialize();

	DatabaseInstance &GetDatabase();
	AttachedDatabase &GetAttached() {
		return db;
	}

	//! Get the WAL of the StorageManager, returns nullptr if in-memory
	optional_ptr<WriteAheadLog> GetWriteAheadLog() {
		return wal.get();
	}

	string GetDBPath() {
		return path;
	}
	bool InMemory();

	virtual bool AutomaticCheckpoint(idx_t estimated_wal_bytes) = 0;
	virtual unique_ptr<StorageCommitState> GenStorageCommitState(Transaction &transaction, bool checkpoint) = 0;
	virtual bool IsCheckpointClean(MetaBlockPointer checkpoint_id) = 0;
	virtual void CreateCheckpoint(bool delete_wal = false, bool force_checkpoint = false) = 0;
	virtual DatabaseSize GetDatabaseSize() = 0;
	virtual shared_ptr<TableIOManager> GetTableIOManager(BoundCreateTableInfo *info) = 0;

protected:
	virtual void LoadDatabase() = 0;

protected:
	//! The database this storagemanager belongs to
	AttachedDatabase &db;
	//! The path of the database
	string path;
	//! The WriteAheadLog of the storage manager
	unique_ptr<WriteAheadLog> wal;
	//! Whether or not the database is opened in read-only mode
	bool read_only;

public:
	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

//! Stores database in a single file.
class SingleFileStorageManager : public StorageManager {
public:
	SingleFileStorageManager(AttachedDatabase &db, string path, bool read_only);

	//! The BlockManager to read/store meta information and data in blocks
	unique_ptr<BlockManager> block_manager;
	//! TableIoManager
	unique_ptr<TableIOManager> table_io_manager;

public:
	bool AutomaticCheckpoint(idx_t estimated_wal_bytes) override;
	unique_ptr<StorageCommitState> GenStorageCommitState(Transaction &transaction, bool checkpoint) override;
	bool IsCheckpointClean(MetaBlockPointer checkpoint_id) override;
	void CreateCheckpoint(bool delete_wal, bool force_checkpoint) override;
	DatabaseSize GetDatabaseSize() override;
	shared_ptr<TableIOManager> GetTableIOManager(BoundCreateTableInfo *info) override;

protected:
	void LoadDatabase() override;
};
} // namespace duckdb
