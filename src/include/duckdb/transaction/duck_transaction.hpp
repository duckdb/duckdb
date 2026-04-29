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
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/transaction/undo_buffer.hpp"
#include "duckdb/common/enums/active_transaction_state.hpp"

namespace duckdb {
class CheckpointLock;
class CommitDropState;
class DuckTableEntry;
class RowGroupCollection;
class RowVersionManager;
class DuckTransactionManager;
class StorageLockKey;
class StorageCommitState;
class TransactionManager;
struct DataTableInfo;
struct UndoBufferProperties;

struct CommitInfo {
	transaction_t commit_id;
	ActiveTransactionState active_transactions = ActiveTransactionState::UNSET;
	optional_ptr<CommitDropState> drop_state;
};

//! RAII guard returned by DuckTransaction::LockStatement. Holds both the unique_lock and a
//! shared_ptr<mutex> "keeper" so the underlying mutex outlives the DuckTransaction itself —
//! a COMMIT may destroy the DuckTransaction before this guard naturally goes out of scope.
//! Member declaration order matters: `guard` is declared after `keeper`, so destruction
//! (reverse of declaration) unlocks before dropping the keeper.
struct StatementGuard {
	StatementGuard() = default;
	StatementGuard(shared_ptr<mutex> keeper_p, unique_lock<mutex> guard_p)
	    : keeper(std::move(keeper_p)), guard(std::move(guard_p)) {
	}
	StatementGuard(StatementGuard &&) = default;
	StatementGuard &operator=(StatementGuard &&) = default;
	StatementGuard(const StatementGuard &) = delete;
	StatementGuard &operator=(const StatementGuard &) = delete;

	shared_ptr<mutex> keeper;
	unique_lock<mutex> guard;
};

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

	void SetModifications(DatabaseModificationType type) override;

	bool ShouldWriteToWAL(AttachedDatabase &db);
	ErrorData WriteToWAL(ClientContext &context, AttachedDatabase &db,
	                     unique_ptr<StorageCommitState> &commit_state) noexcept;
	//! Commit the current transaction with the given commit identifier. Returns an error message if the transaction
	//! commit failed, or an empty string if the commit was successful
	ErrorData Commit(AttachedDatabase &db, CommitInfo &commit_info,
	                 unique_ptr<StorageCommitState> commit_state) noexcept;
	//! Returns whether or not a commit of this transaction should trigger an automatic checkpoint
	bool AutomaticCheckpoint(AttachedDatabase &db, const UndoBufferProperties &properties);

	//! Rollback
	ErrorData Rollback();
	//! Cleanup the undo buffer
	void Cleanup(transaction_t lowest_active_transaction);

	bool ChangesMade();
	UndoBufferProperties GetUndoProperties();

	void PushDelete(DuckTableEntry &table_entry, RowVersionManager &info, idx_t vector_idx, row_t rows[], idx_t count,
	                idx_t base_row);
	void PushSequenceUsage(SequenceCatalogEntry &entry, const SequenceData &data);
	void PushAppend(DuckTableEntry &table_entry, idx_t row_start, idx_t row_count);
	UndoBufferReference CreateUpdateInfo(DuckTableEntry &table_entry, idx_t type_size, idx_t entries,
	                                     idx_t row_group_start);

	DuckTransactionManager &GetTransactionManager();
	bool IsDuckTransaction() const override {
		return true;
	}

	unique_ptr<StorageLockKey> TryGetCheckpointLock();

	//! Get a shared lock on a table
	shared_ptr<CheckpointLock> SharedLockTable(DataTableInfo &info);

	void SetIsCheckpointTransaction() {
		is_checkpoint_transaction = true;
	}

	//! Whether this transaction is currently shared with more than one MetaTransaction.
	bool IsShared() const {
		return participant_count.load() > 1;
	}
	idx_t ParticipantCount() const {
		return participant_count.load();
	}
	bool RollbackRequested() const {
		return rollback_requested.load();
	}
	//! Attempt to add a participant. Returns false if the transaction has already been finalized
	//! (participant_count reached 0 - cannot revive). Used by SET TRANSACTION SNAPSHOT import.
	bool TryAddParticipant();
	//! Decrement the participant count. If rollback is true, sets rollback_requested. Returns
	//! true iff this caller is the last detacher (participant_count went to 0) and is therefore
	//! responsible for finalizing the underlying DuckTransaction at the storage layer.
	bool Detach(bool rollback);
	//! Undo a TryAddParticipant() that was not followed by a successful import. Decrements
	//! participant_count without setting rollback_requested. Must NOT be used as a substitute
	//! for Detach() — this is purely a setup-failure rollback.
	//!
	//! If the owner detached between our TryAddParticipant and this CancelParticipation, this
	//! caller becomes the last detacher (count 1 → 0) and is responsible for driving storage-
	//! layer finalization, otherwise the DuckTransaction would leak in DuckTransactionManager's
	//! active list. We honor rollback_requested if set, otherwise commit.
	void CancelParticipation(ClientContext &context);
	//! Drive the storage-layer finalize for this MetaTransaction's reference. Decrements the
	//! participant count. If this caller is the last to detach, runs CommitTransaction or
	//! RollbackTransaction through the per-database TransactionManager. `vote_rollback=true`
	//! sticks `rollback_requested` so any concurrent COMMIT attempt fails eagerly and the
	//! eventual last detacher rolls back regardless of how it itself voted.
	//!
	//! This is the single entry point for ending a MetaTransaction's reference to a
	//! DuckTransaction — both for the unshared owner case (count 1 → 0, fires finalize) and
	//! shared cases (each non-last detacher just decrements).
	ErrorData Finalize(ClientContext &context, bool vote_rollback);

	//! Acquire the per-DuckTransaction statement lock. Always returns a held lock — taking the
	//! mutex unconditionally (rather than only when shared) is required for correctness: when
	//! a participant joins via JOIN TRANSACTION the owner may already be mid-query, and a
	//! conditional acquire would let owner and participant mutate LocalStorage / UndoBuffer
	//! concurrently across the 1→2 transition. Cost is one uncontended-mutex acquire per query
	//! per opened DuckTransaction. The returned guard pins the underlying mutex via shared_ptr
	//! so it survives DuckTransaction destruction (a COMMIT may destroy the DuckTransaction
	//! before the guard's natural end-of-query release).
	StatementGuard LockStatement();

private:
	//! The undo buffer is used to store old versions of rows that are updated
	//! or deleted
	UndoBuffer undo_buffer;
	//! The set of uncommitted appends for the transaction
	unique_ptr<LocalStorage> storage;
	//! Lock that prevents checkpoints from starting
	unique_ptr<StorageLockKey> checkpoint_lock;
	//! Lock that prevents vacuums from starting
	unique_ptr<StorageLockKey> vacuum_lock;
	//! Lock for accessing sequence_usage
	mutex sequence_lock;
	//! Map of all sequences that were used during the transaction and the value they had in this transaction
	reference_map_t<SequenceCatalogEntry, reference<SequenceValue>> sequence_usage;
	//! Lock for the active_locks map
	mutex active_locks_lock;
	struct ActiveTableLock {
		mutex checkpoint_lock_mutex; // protects access to the checkpoint_lock field in this class
		weak_ptr<CheckpointLock> checkpoint_lock;
	};
	//! Active locks on tables
	reference_map_t<DataTableInfo, unique_ptr<ActiveTableLock>> active_locks;
	//! Flag to prevent auto-checkpointing inside a checkpoint transaction.
	bool is_checkpoint_transaction = false;
	//! Number of MetaTransactions currently referencing this DuckTransaction (1 = owner only,
	//! >1 = shared with one or more participants). Bumped on import, decremented on detach.
	atomic<idx_t> participant_count {1};
	//! Set if any referencing MetaTransaction asked to roll this transaction back. Once set, the
	//! last detacher rolls back instead of committing, and any further COMMIT attempts on this
	//! transaction throw eagerly.
	atomic<bool> rollback_requested {false};
	//! Serializes statement execution across owner + participants while the transaction is shared.
	//! Preserves the single-writer-per-DuckTransaction invariant (LocalStorage / UndoBuffer / per-
	//! table local indexes were designed assuming one query thread mutates at a time). Held via
	//! shared_ptr so the lock outlives the DuckTransaction — explicit COMMIT may destroy the
	//! DuckTransaction before its statement guard naturally unwinds, and we mustn't unlock a
	//! freed mutex. Initialized in the constructor.
	shared_ptr<mutex> statement_lock;
};

} // namespace duckdb
