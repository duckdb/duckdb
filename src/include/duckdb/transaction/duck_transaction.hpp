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
	//! Highest active query when the transaction finished, used for cleaning up
	transaction_t highest_active_query;

	atomic<idx_t> catalog_version;

public:
	static DuckTransaction &Get(ClientContext &context, AttachedDatabase &db);
	static DuckTransaction &Get(ClientContext &context, Catalog &catalog);
	LocalStorage &GetLocalStorage();

	void PushCatalogEntry(CatalogEntry &entry, data_ptr_t extra_data, idx_t extra_data_size);

	void SetReadWrite() override;

	bool ShouldWriteToWAL(AttachedDatabase &db);
	ErrorData WriteToWAL(AttachedDatabase &db, unique_ptr<StorageCommitState> &commit_state) noexcept;
	//! Commit the current transaction with the given commit identifier. Returns an error message if the transaction
	//! commit failed, or an empty string if the commit was sucessful
	ErrorData Commit(AttachedDatabase &db, transaction_t commit_id,
	                 unique_ptr<StorageCommitState> commit_state) noexcept;
	//! Returns whether or not a commit of this transaction should trigger an automatic checkpoint
	bool AutomaticCheckpoint(AttachedDatabase &db, const UndoBufferProperties &properties);

	//! Rollback
	void Rollback() noexcept;
	//! Cleanup the undo buffer
	void Cleanup(transaction_t lowest_active_transaction);

	bool ChangesMade();
	UndoBufferProperties GetUndoProperties();

	void PushDelete(DataTable &table, RowVersionManager &info, idx_t vector_idx, row_t rows[], idx_t count,
	                idx_t base_row);
	void PushSequenceUsage(SequenceCatalogEntry &entry, const SequenceData &data);
	void PushAppend(DataTable &table, idx_t row_start, idx_t row_count);
	UpdateInfo *CreateUpdateInfo(idx_t type_size, idx_t entries);

	bool IsDuckTransaction() const override {
		return true;
	}

	unique_ptr<StorageLockKey> TryGetCheckpointLock();
	bool HasWriteLock() const {
		return write_lock.get();
	}

	void UpdateCollection(shared_ptr<RowGroupCollection> &collection);

	//! Get a shared lock on a table
	shared_ptr<CheckpointLock> SharedLockTable(DataTableInfo &info);

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
	//! Collections that are updated by this transaction
	reference_map_t<RowGroupCollection, shared_ptr<RowGroupCollection>> updated_collections;
	//! Lock for the active_locks map
	mutex active_locks_lock;
	//! Active locks on tables
	reference_map_t<DataTableInfo, weak_ptr<CheckpointLock>> active_locks;
};

} // namespace duckdb
