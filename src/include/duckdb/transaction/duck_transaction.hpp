//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/duck_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/transaction/undo_buffer.hpp"

namespace duckdb {
class CheckpointLock;
class RowGroupCollection;
class RowVersionManager;
class DuckTransactionManager;
class StorageLockKey;
class StorageCommitState;
struct DataTableInfo;
struct UndoBufferProperties;

class DuckTransaction : public Transaction {
public:
	DuckTransaction(DuckTransactionManager &manager, ClientContext &context, transaction_t start_time,
	                transaction_t transaction_id, idx_t catalog_version);
	~DuckTransaction() override;

	//! The start timestamp of this transaction
	transaction_t start_time;
	//! The transaction id of this transaction
	transaction_t transaction_id;
	//! The commit id of this transaction, if it has successfully been committed
	transaction_t commit_id;

	atomic<idx_t> catalog_version;

	//! Transactions undergo Cleanup, after (1) removing them directly in RemoveTransaction,
	//! or (2) after they enter cleanup_queue.
	//! Some (after rollback) enter cleanup_queue, but do not require Cleanup.
	bool awaiting_cleanup;

public:
	static DuckTransaction &Get(ClientContext &context, AttachedDatabase &db);
	static DuckTransaction &Get(ClientContext &context, Catalog &catalog);
	LocalStorage &GetLocalStorage();

	void PushCatalogEntry(CatalogEntry &entry, data_ptr_t extra_data, idx_t extra_data_size);
	void PushAttach(AttachedDatabase &db);

	void SetReadWrite() override;

	bool ShouldWriteToWAL(AttachedDatabase &db);
	ErrorData WriteToWAL(ClientContext &context, AttachedDatabase &db,
	                     unique_ptr<StorageCommitState> &commit_state) noexcept;
	//! Commit the current transaction with the given commit identifier. Returns an error message if the transaction
	//! commit failed, or an empty string if the commit was sucessful
	ErrorData Commit(AttachedDatabase &db, transaction_t commit_id,
	                 unique_ptr<StorageCommitState> commit_state) noexcept;
	//! Returns whether or not a commit of this transaction should trigger an automatic checkpoint
	bool AutomaticCheckpoint(AttachedDatabase &db, const UndoBufferProperties &properties);

	//! Rollback
	ErrorData Rollback();
	//! Cleanup the undo buffer
	void Cleanup(transaction_t lowest_active_transaction);

	bool ChangesMade();
	UndoBufferProperties GetUndoProperties();

	void PushDelete(DataTable &table, RowVersionManager &info, idx_t vector_idx, row_t rows[], idx_t count,
	                idx_t base_row);
	void PushSequenceUsage(SequenceCatalogEntry &entry, const SequenceData &data);
	void PushAppend(DataTable &table, idx_t row_start, idx_t row_count);
	UndoBufferReference CreateUpdateInfo(idx_t type_size, DataTable &data_table, idx_t entries, idx_t row_group_start);

	bool IsDuckTransaction() const override {
		return true;
	}

	unique_ptr<StorageLockKey> TryGetCheckpointLock();
	bool HasWriteLock() const {
		return write_lock.get();
	}

	//! Get a shared lock on a table
	shared_ptr<CheckpointLock> SharedLockTable(DataTableInfo &info);

	//! Hold an owning reference of the table, needed to safely reference it inside the transaction commit/undo logic
	void ModifyTable(DataTable &tbl);

private:
	DuckTransactionManager &transaction_manager;
	//! The undo buffer is used to store old versions of rows that are updated
	//! or deleted
	UndoBuffer undo_buffer;
	//! The set of uncommitted appends for the transaction
	unique_ptr<LocalStorage> storage;
	//! Write lock
	unique_ptr<StorageLockKey> write_lock;
	//! Lock for accessing sequence_usage
	mutex sequence_lock;
	//! Map of all sequences that were used during the transaction and the value they had in this transaction
	reference_map_t<SequenceCatalogEntry, reference<SequenceValue>> sequence_usage;
	//! Lock for modified_tables
	mutex modified_tables_lock;
	//! Tables that are modified by this transaction
	reference_map_t<DataTable, shared_ptr<DataTable>> modified_tables;
	//! Lock for the active_locks map
	mutex active_locks_lock;
	struct ActiveTableLock {
		mutex checkpoint_lock_mutex; // protects access to the checkpoint_lock field in this class
		weak_ptr<CheckpointLock> checkpoint_lock;
	};
	//! Active locks on tables
	reference_map_t<DataTableInfo, unique_ptr<ActiveTableLock>> active_locks;
};

} // namespace duckdb
