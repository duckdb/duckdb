//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/data_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/table_index_list.hpp"

namespace duckdb {
class AttachedDatabase;
class DatabaseInstance;
struct CheckpointOptions;
class TableIOManager;
class RowGroupCollection;

struct DataTableInfo {
	friend class DataTable;

public:
	DataTableInfo(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, vector<Identifier> schema_path,
	              Identifier table);

	//! Bind unknown indexes throwing an exception if binding fails.
	//! Only binds the specified index type, or all if no type is specified. With skip_mismatched_keys, an index
	//! whose stored keys bind to other types here is left unbound instead of refused.
	void BindIndexes(ClientContext &context, const optional<string> &index_type = {},
	                 bool skip_mismatched_keys = false);

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
	bool AppendRequiresNewRowGroup(RowGroupCollection &collection, optional_idx checkpoint_id);
	optional_idx CheckpointRowGroupCount(const CheckpointOptions &options) const;
	void VerifyIndexBuffers() const;

	Identifier GetSchemaName();
	//! The full (possibly nested) schema path of the table
	const vector<Identifier> &GetSchemaPath() const;
	Identifier GetTableName();
	void SetTableName(Identifier name);
	//! The last commit that changed rows of the table, set under the commit lock
	void SetLastChangeCommit(transaction_t commit_id) {
		last_change_commit = commit_id;
	}
	//! Records a running CREATE INDEX scan without deltas, which started after start_commit
	void AddIndexScan(transaction_t start_commit);
	void RemoveIndexScan(transaction_t start_commit);
	//! Whether a commit changed the table after the oldest running CREATE INDEX scan without deltas started
	bool ChangedDuringIndexScan();

private:
	//! The database instance of the table
	AttachedDatabase &db;
	//! The table IO manager
	shared_ptr<TableIOManager> table_io_manager;
	//! Lock for modifying the name
	mutex name_lock;
	//! The (possibly nested) schema path of the table, outermost schema first
	vector<Identifier> schema_path;
	//! The name of the table
	Identifier table;
	//! The physical list of indexes of this table
	TableIndexList indexes;
	//! Index storage information of the indexes created by this table
	vector<IndexStorageInfo> index_storage_infos;
	//! The last seen checkpoint while doing a concurrent operation, if any
	optional_idx last_seen_checkpoint;
	//! The amount of row groups the checkpoint is processing
	optional_idx checkpoint_row_group_count;
	//! See SetLastChangeCommit()
	atomic<transaction_t> last_change_commit {0};
	//! Lock for index_scan_starts
	mutex index_scan_lock;
	//! The start commits of the running CREATE INDEX scans without deltas
	multiset<transaction_t> index_scan_starts;
};

} // namespace duckdb
