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
#include "duckdb/common/deque.hpp"

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
	//! Wait until every published commit is durable; cancellable when a client context is given
	void WaitForDurability(optional_ptr<ClientContext> context = nullptr);
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

	//! Returns the current version of the catalog (incremented whenever anything changes, not stored between restarts)
	DUCKDB_API idx_t GetCatalogVersion(Transaction &transaction);

	void PushCatalogEntry(Transaction &transaction_p, CatalogEntry &entry, data_ptr_t extra_data = nullptr,
	                      idx_t extra_data_size = 0);
	void PushAttach(Transaction &transaction_p, AttachedDatabase &db);

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

	struct TransactionHorizon {
		transaction_t lowest_start_time = TRANSACTION_ID_START;
		transaction_t lowest_transaction_id = MAX_TRANSACTION_ID;
	};
	//! Lowest start time / id over the active transactions, pinned at the durable bound; also
	//! refreshes lowest_active_start/id. Caller holds the transaction lock
	TransactionHorizon UpdateTransactionHorizon(optional_ptr<DuckTransaction> exclude);
	//! Move committed transactions no snapshot can need anymore into the cleanup info
	void SweepCommittedTransactions(transaction_t lowest_start_time, DuckCleanupInfo &cleanup_info);
	//! Queue a composed cleanup; the transaction lock keeps catalog cleanups in commit order
	void QueueCleanup(unique_ptr<DuckCleanupInfo> cleanup_info);

	//! Register a published commit whose flush marker is not yet synced (transaction + WAL lock held)
	void RegisterUnsyncedCommit(transaction_t commit_id, idx_t wal_offset, idx_t catalog_version);
	//! Advance the durable bound over the completed sync and drop this thread's entry; returns
	//! whether the bound advanced
	bool FinishCommitDurability(transaction_t commit_id, idx_t synced_offset);
	//! Mark that a WAL sync has failed, waking up durability waiters
	void MarkDurabilityFailed();
	//! Sweep transactions pinned only by not-yet-durable commits; nothing else re-triggers it
	void GarbageCollectDurableTransactions();
	bool HasUnsyncedCommits();
	struct SnapshotBound {
		//! The highest start time that observes only durable commits
		transaction_t start_time = MAX_TRANSACTION_ID;
		//! The catalog version that snapshot observes. Prepared statements compare versions for
		//! equality, so this has to be exact: any other value can match a plan bound against a
		//! different catalog state and skip a re-bind that was needed
		idx_t catalog_version = DConstants::INVALID_INDEX;
	};
	//! The snapshot bound while commits are pending durability (unbounded otherwise)
	SnapshotBound GetSnapshotBound();

	//! Commits are published before their WAL flush marker is synced: until then they are tracked
	//! here and new snapshots are bounded below them, so no transaction can observe a commit a
	//! crash could still lose. An entry is removed only by its own thread after it leaves
	//! WriteAheadLog::SyncUpTo, making WaitForDurability a quiescence barrier for checkpoints.
	//! Teardown does not rely on that drain - a committing thread holds the AttachedDatabase - so
	//! the destructor asserts the queue is empty instead
	struct UnsyncedCommit {
		transaction_t commit_id;
		//! The WAL offset covering the commit's flush marker
		idx_t wal_offset;
		//! The committed catalog version just before this commit published
		idx_t catalog_version;
	};

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

	//! Protects all durability state below
	mutex durability_lock;
	//! Published commits whose flush marker is not yet durable (in commit order)
	deque<UnsyncedCommit> unsynced_commits;
	//! Signalled when unsynced_commits becomes empty, or when a sync fails
	std::condition_variable durability_cv;
	//! The highest commit id for which it and all lower commits are durable
	transaction_t durable_commit_bound = 0;
	//! Set when a WAL sync has failed (the database is poisoned)
	bool durability_failed = false;

	atomic<idx_t> last_uncommitted_catalog_version = {TRANSACTION_ID_START};
	idx_t last_committed_version = 0;

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
