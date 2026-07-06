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
	//! otherwise dereference freed memory.
	void RetireObject(shared_ptr<void> retired_object);

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

	//! Register a commit that is about to write its WAL flush marker and defer its publish. Assigns and returns a
	//! monotonic publish sequence (used by WaitForPublishTurn to publish in order). Must be called while holding the
	//! WAL append lock so sequences are handed out in WAL-append order.
	idx_t RegisterPendingCommit();
	//! Wait until all earlier pending commits have been published. Publishing in WAL (publish-sequence) order
	//! keeps recently_committed_transactions ordered on commit_id and matches WAL replay order.
	void WaitForPublishTurn(idx_t publish_seq);
	//! Mark a pending commit as published (or abandoned on fatal failure) and wake up waiters.
	void FinishPendingCommit(idx_t publish_seq);

	//! RAII backstop for a registered pending commit: guarantees FinishPendingCommit runs exactly once even if an
	//! unexpected exception unwinds CommitTransaction - a leaked pending entry would hang all later deferred commits.
	class PendingCommitGuard {
	public:
		~PendingCommitGuard() {
			Finish();
		}

	public:
		void Register(DuckTransactionManager &manager_p, idx_t publish_seq_p) {
			manager = &manager_p;
			publish_seq = publish_seq_p;
		}
		//! Finish the pending commit now (idempotent, no-op if never registered)
		void Finish() {
			if (manager) {
				manager->FinishPendingCommit(publish_seq);
				manager = nullptr;
			}
		}

	private:
		optional_ptr<DuckTransactionManager> manager;
		idx_t publish_seq = 0;
	};

private:
	//! The current start timestamp used by transactions. Atomic so RetireObject can read the epoch horizon without
	//! taking the transaction lock (which a checkpoint calling RetireObject must not block on). Incremented only
	//! under transaction_lock, so the increments stay serialized.
	atomic<transaction_t> current_start_timestamp;
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

	//! Protects pending_commit_publishes and next_publish_sequence.
	mutex publish_lock;
	//! Signalled whenever a pending commit is published.
	std::condition_variable publish_cv;
	//! Publish sequence numbers of commits that are durable in the WAL (flush marker written) but not yet
	//! published/visible. Ordered by publish sequence (= WAL order) so publishes happen in WAL order.
	//! Lock ordering: transaction_lock -> publish_lock. Waiting on publish_cv requires holding neither
	//! the transaction lock nor the WAL lock.
	set<idx_t> pending_commit_publishes;
	//! Monotonic sequence assigned to deferred commits in RegisterPendingCommit (under publish_lock; callers hold the
	//! WAL append lock so sequences follow WAL order), used to order their publishes. Decoupled from the commit
	//! timestamp, which is only assigned at publish time so a starting transaction can never observe a commit id
	//! that is not yet published.
	idx_t next_publish_sequence = 1;

	//! Objects (row groups / update segments) retired by a checkpoint, each tagged with the start-timestamp horizon
	//! at retirement: once lowest_active_start reaches that epoch, no transaction that existed at retirement is still
	//! active, so no undo buffer can reference the object's UpdateSegments and it is freed (see RetireObject).
	mutex retired_lock;
	vector<pair<transaction_t, shared_ptr<void>>> retired_objects;

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
