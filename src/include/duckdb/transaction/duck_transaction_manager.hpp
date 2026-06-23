//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/duck_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/common/enums/checkpoint_type.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/set.hpp"

#include <condition_variable>

namespace duckdb {
class DuckTransactionManager;
class DuckTransaction;
struct UndoBufferProperties;

//! CleanupInfo collects transactions awaiting cleanup.
//! This ensures we can clean up after releasing the transaction lock.
struct DuckCleanupInfo {
	//! All transactions in a cleanup info share the same lowest_start_time.
	transaction_t lowest_start_time;
	vector<unique_ptr<DuckTransaction>> transactions;

	void Cleanup();
	bool ScheduleCleanup() noexcept;
};

//! The Transaction Manager is responsible for creating and managing
//! transactions
class DuckTransactionManager : public TransactionManager {
public:
	explicit DuckTransactionManager(AttachedDatabase &db);
	~DuckTransactionManager() override;

public:
	static DuckTransactionManager &Get(AttachedDatabase &db);

	//! Start a new transaction
	Transaction &StartTransaction(ClientContext &context) override;
	//! Commit the given transaction
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	//! Rollback the given transaction
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

	transaction_t LowestActiveId() const {
		return lowest_active_id;
	}
	transaction_t LowestActiveStart() const {
		return lowest_active_start;
	}
	transaction_t GetLastCommit() const {
		return last_commit;
	}
	transaction_t GetActiveCheckpoint() const {
		return active_checkpoint;
	}
	void SetActiveCheckpoint(transaction_t checkpoint_id);
	void ResetActiveCheckpoint();

	bool IsDuckTransactionManager() override {
		return true;
	}

	//! Obtains a shared lock to the checkpoint lock
	unique_ptr<StorageLockKey> SharedCheckpointLock();
	//! Try to obtain an exclusive checkpoint lock
	unique_ptr<StorageLockKey> TryGetCheckpointLock();
	unique_ptr<StorageLockKey> TryUpgradeCheckpointLock(StorageLockKey &lock);
	unique_ptr<StorageLockKey> SharedVacuumLock();
	unique_ptr<StorageLockKey> TryGetVacuumLock();

	//! Keep an object (a row group whose updates a checkpoint replaced) alive until no transaction can still
	//! reference it through its undo buffer. A committed transaction's undo holds a raw pointer to the row group's
	//! UpdateSegment (UpdateInfo::segment); the checkpoint frees the old row group, so cleanup of that undo would
	//! otherwise dereference freed memory. The retained object is released once the database becomes quiescent
	//! (no active transactions), at which point all undo has been cleaned up.
	void RetireAfterCheckpoint(shared_ptr<void> retired_object);

	//! Returns the current version of the catalog (incremented whenever anything changes, not stored between restarts)
	DUCKDB_API idx_t GetCatalogVersion(Transaction &transaction);

	void PushCatalogEntry(Transaction &transaction_p, CatalogEntry &entry, data_ptr_t extra_data = nullptr,
	                      idx_t extra_data_size = 0);
	void PushAttach(Transaction &transaction_p, AttachedDatabase &db);

	//! Acquire the WAL lock EXCLUSIVELY, returning the lock handle. Used by DDL (ALTER/DROP/CREATE INDEX) to attach a
	//! new catalog version: a deferred (group) commit validates against the catalog before writing its WAL flush
	//! marker and holds the WAL lock SHARED until it publishes, so taking the WAL lock exclusively here both drains
	//! all in-flight deferred commits and blocks new ones until the returned handle is released - keeping that
	//! validation authoritative. Writer priority (see StorageLock) prevents a steady stream of commits from starving
	//! the DDL. The caller must NOT already hold the WAL lock (the lock is not reentrant).
	unique_ptr<StorageLockKey> BlockPendingCommits();

protected:
	struct CheckpointDecision {
		explicit CheckpointDecision(string reason_p);
		explicit CheckpointDecision(CheckpointType type);
		~CheckpointDecision();

		bool can_checkpoint;
		string reason;
		CheckpointType type;
	};

private:
	//! Generates a new commit timestamp
	transaction_t GetCommitTimestamp();
	//! Remove the given transaction from the list of active transactions
	unique_ptr<DuckCleanupInfo> RemoveTransaction(DuckTransaction &transaction) noexcept;
	//! Remove the given transaction from the list of active transactions
	unique_ptr<DuckCleanupInfo> RemoveTransaction(DuckTransaction &transaction, bool store_transaction) noexcept;

	//! Whether or not we can checkpoint
	CheckpointDecision CanCheckpoint(DuckTransaction &transaction, unique_ptr<StorageLockKey> &checkpoint_lock,
	                                 const UndoBufferProperties &properties);
	//! Get the checkpoint type of an automatic checkpoint
	CheckpointDecision GetCheckpointType(DuckTransaction &transaction, const UndoBufferProperties &undo_properties);

	bool HasOtherTransactions(DuckTransaction &transaction);
	void CleanupTransactions();

	//! Register a commit that is about to write its WAL flush marker and defer its publish, identified by a
	//! monotonic publish sequence number assigned in WAL order (used by WaitForPublishTurn to publish in order).
	void RegisterPendingCommit(transaction_t publish_seq);
	//! Wait until all earlier pending commits have been published. Publishing in WAL (publish-sequence) order
	//! keeps recently_committed_transactions ordered on commit_id and matches WAL replay order.
	void WaitForPublishTurn(transaction_t publish_seq);
	//! Mark a pending commit as published (or abandoned on fatal failure) and wake up waiters.
	void FinishPendingCommit(transaction_t publish_seq);

private:
	//! The current start timestamp used by transactions
	transaction_t current_start_timestamp;
	//! The current transaction ID used by transactions
	transaction_t current_transaction_id;
	//! The lowest active transaction id
	atomic<transaction_t> lowest_active_id;
	//! The lowest active transaction timestamp
	atomic<transaction_t> lowest_active_start;
	//! The last commit timestamp
	atomic<transaction_t> last_commit;
	//! The currently active checkpoint
	atomic<transaction_t> active_checkpoint;
	//! Set of currently running transactions
	vector<unique_ptr<DuckTransaction>> active_transactions;
	//! Set of recently committed transactions
	vector<unique_ptr<DuckTransaction>> recently_committed_transactions;
	//! The lock used for transaction operations
	mutex transaction_lock;
	//! The checkpoint lock
	StorageLock checkpoint_lock;
	//! The vacuum lock - necessary to start vacuum operations
	StorageLock vacuum_lock;
	//! Lock necessary to start transactions only - used by FORCE CHECKPOINT to prevent new transactions from starting
	mutex start_transaction_lock;

	atomic<idx_t> last_uncommitted_catalog_version = {TRANSACTION_ID_START};
	idx_t last_committed_version = 0;

	//! Protects pending_commit_publishes.
	mutex publish_lock;
	//! Signalled whenever a pending commit is published.
	std::condition_variable publish_cv;
	//! Publish sequence numbers of commits that are durable in the WAL (flush marker written) but not yet
	//! published/visible. Ordered by publish sequence (= WAL order) so publishes happen in WAL order.
	//! Lock ordering: transaction_lock -> publish_lock. Waiting on publish_cv requires holding neither
	//! the transaction lock nor the WAL lock.
	set<transaction_t> pending_commit_publishes;
	//! Monotonic sequence assigned to deferred commits in WAL order (under the transaction lock), used to order
	//! their publishes. Decoupled from the commit timestamp, which is only assigned at publish time so that a
	//! starting transaction can never observe a commit id that is not yet published.
	transaction_t next_publish_sequence = 1;

	//! Row groups retired by a checkpoint, kept alive until the database is quiescent so that undo buffers can no
	//! longer reference their UpdateSegments (see RetireAfterCheckpoint).
	mutex retired_lock;
	vector<shared_ptr<void>> retired_after_checkpoint;

	//! Only one cleanup can be active at any time.
	mutex cleanup_lock;
	//! Changes to the cleanup queue must be synchronized.
	mutex cleanup_queue_lock;
	//! Cleanups have to happen in-order.
	//! E.g., if one transaction drops a table, and another creates a table,
	//! inverting the cleanup order can result in catalog errors.
	queue<unique_ptr<DuckCleanupInfo>> cleanup_queue;

protected:
	virtual void OnCommitCheckpointDecision(const CheckpointDecision &decision, DuckTransaction &transaction) {
	}
};

} // namespace duckdb
