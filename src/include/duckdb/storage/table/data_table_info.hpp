//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/data_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/table_index_list.hpp"

namespace duckdb {
class AttachedDatabase;
class DatabaseInstance;
class TableIOManager;
class RowGroupCollection;

struct DataTableInfo {
	friend class DataTable;

public:
	DataTableInfo(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, Identifier schema,
	              Identifier table);

	//! Bind unknown indexes throwing an exception if binding fails.
	//! Only binds the specified index type, or all, if nullptr.
	void BindIndexes(ClientContext &context, const char *index_type = nullptr);

	//! Whether or not the table is temporary
	bool IsTemporary() const;

	AttachedDatabase &GetDB() const {
		return db;
	}

	TableIOManager &GetIOManager() {
		return *table_io_manager;
	}

	TableIndexList &GetIndexes() {
		return indexes;
	}
	//! Find and move out an IndexStorageInfo by name from the stored collection.
	IndexStorageInfo ExtractIndexStorageInfo(const Identifier &name);
	unique_ptr<StorageLockKey> GetSharedLock() {
		return checkpoint_lock.GetSharedLock();
	}
	//! Publish gate: a group commit that modifies this table holds it SHARED across publish (which applies its
	//! changes into this table's indexes); a DDL that mutates this table's catalog entry / index list takes it
	//! EXCLUSIVE. This scopes the DDL-vs-commit exclusion to the individual table, so a DDL on one table does not
	//! block commits to other tables. Kept separate from checkpoint_lock so DDL does not serialize against checkpoints.
	unique_ptr<StorageLockKey> GetPublishGateShared() {
		return publish_gate.GetSharedLock();
	}
	unique_ptr<StorageLockKey> GetPublishGateExclusive() {
		return publish_gate.GetExclusiveLock();
	}
	bool AppendRequiresNewRowGroup(RowGroupCollection &collection, transaction_t checkpoint_id);
	optional_idx CheckpointRowGroupCount(const CheckpointOptions &options) const;
	void VerifyIndexBuffers();

	Identifier GetSchemaName();
	Identifier GetTableName();
	void SetTableName(Identifier name);

private:
	//! The database instance of the table
	AttachedDatabase &db;
	//! The table IO manager
	shared_ptr<TableIOManager> table_io_manager;
	//! Lock for modifying the name
	mutex name_lock;
	//! The schema of the table
	Identifier schema;
	//! The name of the table
	Identifier table;
	//! The physical list of indexes of this table
	TableIndexList indexes;
	//! Index storage information of the indexes created by this table
	vector<IndexStorageInfo> index_storage_infos;
	//! Lock held while checkpointing
	StorageLock checkpoint_lock;
	//! Gate excluding DDL on this table from concurrent group-commit publishes (see GetPublishGateShared)
	StorageLock publish_gate;
	//! The last seen checkpoint while doing a concurrent operation, if any
	optional_idx last_seen_checkpoint;
	//! The amount of row groups the checkpoint is processing
	optional_idx checkpoint_row_group_count;
};

} // namespace duckdb
