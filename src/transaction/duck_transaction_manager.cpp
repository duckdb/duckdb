#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "duckdb/main/client_data.hpp"

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/checkpoint/checkpoint_options.hpp"
#include "duckdb/common/string_util.hpp"

#include <thread>

namespace duckdb {

static ErrorData BuildAutocheckpointError(AttachedDatabase &db, const std::exception &ex) {
	ErrorData original(ex);
	string recovery = db.IsInitialDatabase() ? "Reopen the database instance to recover."
	                                         : "Detach and reattach the database to recover.";
	string msg = StringUtil::Format("Transaction COMMIT succeeded and is durable, but the autocheckpoint failed. %s %s",
	                                recovery, original.RawMessage());
	return ErrorData(original.Type(), msg);
}

//! Publishes the identity of the thread holding the start-transaction gate across an in-commit checkpoint, so
//! transactions the checkpoint itself starts can recognize the gate as their own. Clears and unlocks together,
//! also on unwinding, so the marker can never outlive the gate.
struct StartGateHolder {
	StartGateHolder(atomic<std::thread::id> &holder_p, unique_lock<mutex> &gate_p) : holder(holder_p), gate(gate_p) {
		if (gate.owns_lock()) {
			holder.store(std::this_thread::get_id(), std::memory_order_release);
		}
	}
	~StartGateHolder() {
		Release();
	}
	void Release() {
		if (gate.owns_lock()) {
			holder.store(std::thread::id(), std::memory_order_release);
			gate.unlock();
		}
	}
	atomic<std::thread::id> &holder;
	unique_lock<mutex> &gate;
};

void DuckCleanupInfo::Cleanup() {
	for (auto &transaction : transactions) {
		if (transaction->awaiting_cleanup) {
			transaction->Cleanup(lowest_start_time);
		}
	}
}

bool DuckCleanupInfo::ScheduleCleanup() noexcept {
	return !transactions.empty();
}

DuckTransactionManager::DuckTransactionManager(AttachedDatabase &db) : TransactionManager(db) {
	// start timestamp starts at two
	current_start_timestamp = 2;
	// transaction ID starts very high:
	// it should be much higher than the current start timestamp
	// if transaction_id < start_timestamp for any set of active transactions
	// uncommitted data could be read by
	current_transaction_id = TRANSACTION_ID_START;
	lowest_active_id = TRANSACTION_ID_START;
	lowest_active_start = MAX_TRANSACTION_ID;
	active_checkpoint = MAX_TRANSACTION_ID;
	if (!db.GetCatalog().IsDuckCatalog()) {
		// Specifically the StorageManager of the DuckCatalog is relied on, with `db.GetStorageManager`
		throw InternalException("DuckTransactionManager should only be created together with a DuckCatalog");
	}
}

DuckTransactionManager::~DuckTransactionManager() {
}

DuckTransactionManager &DuckTransactionManager::Get(AttachedDatabase &db) {
	auto &transaction_manager = TransactionManager::Get(db);
	if (!transaction_manager.IsDuckTransactionManager()) {
		throw InternalException("Calling DuckTransactionManager::Get on non-DuckDB transaction manager");
	}
	return reinterpret_cast<DuckTransactionManager &>(transaction_manager);
}

Transaction &DuckTransactionManager::StartTransaction(ClientContext &context) {
	// obtain the transaction lock during this function
	auto &meta_transaction = MetaTransaction::Get(context);
	unique_lock<mutex> start_lock(start_transaction_lock, std::defer_lock);
	if (!meta_transaction.IsReadOnly() &&
	    start_gate_holder.load(std::memory_order_acquire) != std::this_thread::get_id()) {
		start_lock.lock();
	}
	lock_guard<mutex> lock(transaction_lock);
	if (current_start_timestamp >= TRANSACTION_ID_START) { // LCOV_EXCL_START
		throw InternalException("Cannot start more transactions, ran out of "
		                        "transaction identifiers!");
	} // LCOV_EXCL_STOP

	// obtain the start time and transaction ID of this transaction
	transaction_t start_time = DurableSnapshotBound(current_start_timestamp++);
	transaction_t transaction_id = current_transaction_id++;
	if (active_transactions.empty()) {
		lowest_active_start = start_time;
		lowest_active_id = transaction_id;
	}

	// create the actual transaction
	auto transaction = make_uniq<DuckTransaction>(*this, context, start_time, transaction_id, last_committed_version);
	auto &transaction_ref = *transaction;

	// store it in the set of active transactions
	active_transactions.push_back(std::move(transaction));
	return transaction_ref;
}

transaction_t DuckTransactionManager::DurableSnapshotBound(transaction_t fresh_start_time) {
	// caller must hold transaction_lock: commits store last_pending_commit in the same critical section that makes
	// them visible, so a snapshot bounded here either misses a commit entirely or sees its durability recorded
	transaction_t durable = last_durable_commit.load(std::memory_order_acquire);
	if (durable >= last_pending_commit.load(std::memory_order_acquire)) {
		return fresh_start_time;
	}
	// commit order equals WAL order equals durability order, so the non-durable commits are exactly the suffix
	// above last_durable_commit: a snapshot just above it sees every durable commit and no non-durable one.
	// Floor at the lowest fresh timestamp: bootstrap catalog entries are committed at timestamp 1 by the system
	// transaction and recreated deterministically on every database open, so they must stay visible even before
	// the first commit is durable, while every real commit id is at least 2 and stays correctly excluded
	return MaxValue<transaction_t>(durable + 1, 2);
}

void DuckTransactionManager::RefreshCheckpointSnapshot(DuckTransaction &transaction) {
	// under transaction_lock every commit at a lower timestamp is fully applied, and the recomputation of
	// lowest_active_start on transaction removal scans the live start times, so raising this one is consistent
	lock_guard<mutex> lock(transaction_lock);
	transaction.start_time = current_start_timestamp++;
}

void DuckTransactionManager::SetActiveCheckpoint(transaction_t checkpoint_id) {
	active_checkpoint = checkpoint_id;
}

void DuckTransactionManager::ResetActiveCheckpoint() {
	active_checkpoint = MAX_TRANSACTION_ID;
}

DuckTransactionManager::CheckpointDecision::CheckpointDecision(string reason_p)
    : can_checkpoint(false), reason(std::move(reason_p)) {
}

DuckTransactionManager::CheckpointDecision::CheckpointDecision(CheckpointType type) : can_checkpoint(true), type(type) {
}

DuckTransactionManager::CheckpointDecision::~CheckpointDecision() {
}

bool DuckTransactionManager::HasOtherTransactions(DuckTransaction &transaction) {
	for (auto &active_transaction : active_transactions) {
		if (!RefersToSameObject(*active_transaction, transaction)) {
			return true;
		}
	}
	return false;
}

DuckTransactionManager::CheckpointDecision
DuckTransactionManager::CanCheckpoint(DuckTransaction &transaction, unique_ptr<StorageLockKey> &lock,
                                      const UndoBufferProperties &undo_properties, unique_lock<mutex> &start_gate,
                                      bool take_start_gate) {
	if (db.IsSystem()) {
		return CheckpointDecision("system transaction");
	}
	if (transaction.IsReadOnly()) {
		return CheckpointDecision("transaction is read-only");
	}
	auto &storage_manager = db.GetStorageManager();
	if (!storage_manager.IsLoaded()) {
		return CheckpointDecision("cannot checkpoint while loading");
	}
	if (!transaction.AutomaticCheckpoint(db, undo_properties)) {
		return CheckpointDecision("no reason to automatically checkpoint");
	}
	if (Settings::Get<DebugSkipCheckpointOnCommitSetting>(db.GetDatabase())) {
		return CheckpointDecision("checkpointing on commit disabled through configuration");
	}
	if (take_start_gate) {
		// a WAL-skipping commit stays non-durable for its whole checkpoint, so block new transactions meanwhile --
		// else a snapshot could be bounded below it and need versions the checkpoint vacuums. Try-only, taken before
		// the exclusive checkpoint lock (same order as Checkpoint(), so no deadlock); on failure the commit falls
		// back to a normal WAL write. WAL-writing commits are durable before their checkpoint and skip the gate
		(void)start_gate.try_lock();
	}
	// let the in-flight group fsyncs land and run the cleanups the durable horizon held back: committed but
	// not-yet-cleaned transactions keep their shared checkpoint lock, and would otherwise starve the exclusive
	// acquisition below when no later transaction re-evaluates the cleanup threshold
	WaitForInFlightCommits();
	DrainCleanups(false);
	// try to lock the checkpoint lock
	lock = transaction.TryGetCheckpointLock();
	if (!lock) {
		if (start_gate.owns_lock()) {
			start_gate.unlock();
		}
		return CheckpointDecision("Failed to obtain checkpoint lock - another thread is writing/checkpointing or "
		                          "another read transaction relies on data that is not yet committed");
	}
	return CheckpointDecision(CheckpointType::FULL_CHECKPOINT);
}

void DuckTransactionManager::WaitForInFlightCommits() {
	// group fsyncs of already-published commits complete on their committers' threads without needing any lock
	// held here, and every published commit raises the durable horizon on its durability path. The target is
	// snapped once, so the wait is bounded by the fsyncs in flight now, not by later commits
	transaction_t target = last_pending_commit.load(std::memory_order_acquire);
	while (last_durable_commit.load(std::memory_order_acquire) < target) {
		std::this_thread::yield();
	}
}

DuckTransactionManager::CheckpointDecision
DuckTransactionManager::GetCheckpointType(DuckTransaction &transaction, const UndoBufferProperties &undo_properties) {
	auto &storage_manager = db.GetStorageManager();
	auto checkpoint_type = CheckpointType::FULL_CHECKPOINT;
	bool has_other_transactions = HasOtherTransactions(transaction);
	if (has_other_transactions) {
		if (undo_properties.has_updates || undo_properties.has_dropped_entries) {
			// if we have made updates/catalog changes in this transaction we cannot checkpoint
			// in the presence of other transactions
			string other_transactions;
			for (auto &active_transaction : active_transactions) {
				if (!RefersToSameObject(*active_transaction, transaction)) {
					if (!other_transactions.empty()) {
						other_transactions += ", ";
					}
					other_transactions += "[" + to_string(active_transaction->transaction_id) + "]";
				}
			}
			if (!other_transactions.empty()) {
				// there are other transactions!
				// these active transactions might need data from BEFORE this transaction
				// we might need to change our strategy here based on what changes THIS transaction has made
				if (undo_properties.has_dropped_entries) {
					// this transaction has changed the catalog - we cannot checkpoint
					return CheckpointDecision(
					    "Transaction has dropped catalog entries and there are other transactions "
					    "active\nActive transactions: " +
					    other_transactions);
				}
				// this transaction has performed updates - we cannot checkpoint
				return CheckpointDecision(
				    "Transaction has performed updates and there are other transactions active\nActive transactions: " +
				    other_transactions);
			}
		}
		// otherwise - we need to do a concurrent checkpoint
		checkpoint_type = CheckpointType::CONCURRENT_CHECKPOINT;
	}
	if (storage_manager.InMemory() && !storage_manager.CompressionIsEnabled()) {
		if (checkpoint_type == CheckpointType::CONCURRENT_CHECKPOINT) {
			return CheckpointDecision("Cannot vacuum, and compression is disabled for in-memory table");
		}
		return CheckpointDecision(CheckpointType::VACUUM_ONLY);
	}
	return CheckpointDecision(checkpoint_type);
}

void DuckTransactionManager::Checkpoint(ClientContext &context, bool force) {
	if (ValidChecker::IsInvalidated(db)) {
		throw IOException("%s", ValidChecker::InvalidatedMessage(db));
	}
	auto &storage_manager = db.GetStorageManager();
	auto current = Transaction::TryGet(context, db);
	if (current) {
		if (force) {
			throw TransactionException(
			    "Cannot FORCE CHECKPOINT: the current transaction has been started for this database");
		} else {
			auto &duck_transaction = current->Cast<DuckTransaction>();
			if (duck_transaction.ChangesMade()) {
				throw TransactionException("Cannot CHECKPOINT: the current transaction has transaction local changes");
			}
		}
	}

	unique_ptr<StorageLockKey> lock;
	// let the in-flight group fsyncs land and run the cleanups the durable horizon held back: committed but
	// not-yet-cleaned transactions keep their shared checkpoint lock and would block the exclusive acquisition
	WaitForInFlightCommits();
	DrainCleanups(false);
	if (!force) {
		// not a force checkpoint
		// try to get the checkpoint lock
		lock = checkpoint_lock.TryGetExclusiveLock();
		if (!lock) {
			// we could not manage to get the lock - cancel
			throw TransactionException("Cannot CHECKPOINT: there are other write transactions active. Try using FORCE "
			                           "CHECKPOINT to wait until all active transactions are finished");
		}

	} else {
		// force checkpoint - wait to get an exclusive lock
		// grab the start_transaction_lock to prevent new transactions from starting
		lock_guard<mutex> start_lock(start_transaction_lock);
		// wait until any active transactions are finished
		while (!lock) {
			context.InterruptCheck();
			WaitForInFlightCommits();
			DrainCleanups(false);
			lock = checkpoint_lock.TryGetExclusiveLock();
		}
	}
	// a full checkpoint (chosen below when no active snapshot needs old data) is safe without blocking new
	// transactions: after the drain the durable horizon covers every pending commit and the exclusive checkpoint
	// lock keeps new write commits out, so a snapshot taken during the checkpoint is fresh
	CheckpointOptions options;
	if (GetLastCommit() > LowestActiveStart()) {
		// we cannot do a full checkpoint if any transaction needs to read old data
		options.type = CheckpointType::CONCURRENT_CHECKPOINT;
	}

	storage_manager.CreateCheckpoint(context, options);
}

unique_ptr<StorageLockKey> DuckTransactionManager::SharedCheckpointLock() {
	return checkpoint_lock.GetSharedLock();
}

unique_ptr<StorageLockKey> DuckTransactionManager::TryUpgradeCheckpointLock(StorageLockKey &lock) {
	return checkpoint_lock.TryUpgradeCheckpointLock(lock);
}

unique_ptr<StorageLockKey> DuckTransactionManager::TryGetCheckpointLock() {
	return checkpoint_lock.TryGetExclusiveLock();
}

unique_ptr<StorageLockKey> DuckTransactionManager::SharedVacuumLock() {
	return vacuum_lock.GetSharedLock();
}

unique_ptr<StorageLockKey> DuckTransactionManager::TryGetVacuumLock() {
	return vacuum_lock.TryGetExclusiveLock();
}

transaction_t DuckTransactionManager::GetCommitTimestamp() {
	return current_start_timestamp++;
}

void DuckTransactionManager::DrainCleanups(bool include_non_durable) {
	// promote up to the minimum live start time, clamped at the durable horizon (like RemoveTransaction), so a commit
	// whose fsync is still in flight keeps the versions a bounded snapshot may still see. Only a checkpoint carrying
	// its own commit (include_non_durable) promotes past the horizon -- its start gate blocks such snapshots
	auto cleanup_info = make_uniq<DuckCleanupInfo>();
	{
		lock_guard<mutex> lock(transaction_lock);
		auto lowest_start_time = TRANSACTION_ID_START;
		for (auto &active_transaction : active_transactions) {
			lowest_start_time = MinValue(lowest_start_time, active_transaction->start_time);
		}
		// refresh the cached lower bound: it may still carry the durable-horizon clamp of an earlier
		// RemoveTransaction, and a stale-low value silently downgrades full checkpoints to concurrent ones
		lowest_active_start = MinValue(lowest_start_time, DurableSnapshotBound(TRANSACTION_ID_START));
		if (!include_non_durable) {
			lowest_start_time = MinValue(lowest_start_time, DurableSnapshotBound(TRANSACTION_ID_START));
		}
		cleanup_info->lowest_start_time = lowest_start_time;
		idx_t i = 0;
		for (; i < recently_committed_transactions.size(); i++) {
			if (recently_committed_transactions[i]->commit_id >= lowest_start_time) {
				break;
			}
			recently_committed_transactions[i]->awaiting_cleanup = true;
			cleanup_info->transactions.push_back(std::move(recently_committed_transactions[i]));
		}
		if (i > 0) {
			auto start = recently_committed_transactions.begin();
			auto end = recently_committed_transactions.begin() + static_cast<int64_t>(i);
			recently_committed_transactions.erase(start, end);
		}
	}
	if (cleanup_info->ScheduleCleanup()) {
		lock_guard<mutex> q_lock(cleanup_queue_lock);
		cleanup_queue.emplace(std::move(cleanup_info));
	}
	CleanupTransactions();
}

void DuckTransactionManager::CleanupTransactions() {
	lock_guard<mutex> c_lock(cleanup_lock);
	while (true) {
		unique_ptr<DuckCleanupInfo> top_cleanup_info;
		{
			lock_guard<mutex> q_lock(cleanup_queue_lock);
			if (cleanup_queue.empty()) {
				// all transactions have been cleaned up - done
				return;
			}
			top_cleanup_info = std::move(cleanup_queue.front());
			cleanup_queue.pop();
		}
		if (top_cleanup_info) {
			top_cleanup_info->Cleanup();
		}
	}
}

ErrorData DuckTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction_p) {
	auto &transaction = transaction_p.Cast<DuckTransaction>();
	// check if we can checkpoint, before taking the transaction lock: for a WAL-skipping commit this acquires
	// start_transaction_lock, which is ordered before transaction_lock (see StartTransaction)
	unique_ptr<StorageLockKey> lock;
	auto undo_properties = transaction.GetUndoProperties();
	const bool wants_skip_wal =
	    undo_properties.estimated_size >= Settings::Get<AutoCheckpointSkipWalThresholdSetting>(context);
	unique_lock<mutex> start_gate(start_transaction_lock, std::defer_lock);
	auto checkpoint_decision = CanCheckpoint(transaction, lock, undo_properties, start_gate, wants_skip_wal);
	StartGateHolder gate_holder(start_gate_holder, start_gate);

	unique_lock<mutex> t_lock(transaction_lock);
	if (!db.IsSystem() && !db.IsTemporary()) {
		if (transaction.ChangesMade()) {
			if (transaction.IsReadOnly()) {
				throw InternalException("Attempting to commit a transaction that is read-only but has made changes - "
				                        "this should not be possible");
			}
		}
	}
	ErrorData error;
	unique_lock<mutex> held_wal_lock;
	// pin the WAL object (captured below while holding the WAL lock) so a concurrent checkpoint that resets it cannot
	// free the object out from under our GroupSync fsync, which runs with the WAL lock released
	shared_ptr<WriteAheadLog> wal_ref;
	unique_ptr<StorageCommitState> commit_state;
	bool skip_wal_write_due_to_checkpoint = false;
	bool wal_written = false;
	transaction_t prior_pending = 0;
	if (checkpoint_decision.can_checkpoint) {
		// we can perform an automatic checkpoint
		// we have two options:
		// either we write to the WAL, in which case we can perform concurrent commits while running
		// OR we skip writing to the WAL, in which case we cannot perform concurrent commits
		// the reason for this is that if we don't write this transactions' changes to the WAL
		// any failure during checkpoint will cause this transactions' changes to be lost,
		// while later concurrent commits will not be
		// this can cause undefined state, as those commits were made assuming this one was already committed.
		// skipping the WAL additionally requires the start gate (see CanCheckpoint): without it the commit falls
		// back to a normal WAL write, so it is durable before the checkpoint runs
		if (wants_skip_wal && start_gate.owns_lock()) {
			skip_wal_write_due_to_checkpoint = true;
		}
	}
	bool should_write_to_wal = transaction.ShouldWriteToWAL(db);
	if (should_write_to_wal) {
		auto &storage_manager = db.GetStorageManager().Cast<SingleFileStorageManager>();
		// if we are committing changes and we are not doing a "checkpoint instead of WAL write"
		// we need to write to the WAL to make the changes durable
		// since WAL writes can take a long time - we grab the WAL lock here and unlock the transaction lock
		// read-only transactions can bypass this branch and start/commit while the WAL write is happening
		// unlock the transaction lock while we write to the WAL
		// note: we can only drop the transaction lock if we are NOT checkpointing
		// if we are checkpointing, we have already made certain decisions (e.g. the CheckpointType)
		t_lock.unlock();
		// grab the WAL lock and hold it until the entire commit is finished
		held_wal_lock = storage_manager.GetWALLock();
		wal_ref = storage_manager.GetWALShared();

		// Commit the changes to the WAL.
		if (!skip_wal_write_due_to_checkpoint) {
			error = transaction.WriteToWAL(context, db, commit_state);
			wal_written = true;
		}

		// after we finish writing to the WAL we grab the transaction lock again
		t_lock.lock();
	}
	if (!error.HasError() && checkpoint_decision.can_checkpoint) {
		// now that we have the transaction lock again, new transactions can't start
		// figure out the checkpoint type now
		checkpoint_decision = GetCheckpointType(transaction, undo_properties);
		if (should_write_to_wal && skip_wal_write_due_to_checkpoint && !checkpoint_decision.can_checkpoint) {
			// we have not written to the WAL but we have now realized we can't checkpoint after all
			// in order to commit we need backpeddle and write to the WAL after all
			D_ASSERT(held_wal_lock.owns_lock());
			// unlock the transaction lock while we are writing to the WAL
			t_lock.unlock();
			error = transaction.WriteToWAL(context, db, commit_state);
			wal_written = true;
			t_lock.lock();
			skip_wal_write_due_to_checkpoint = false;
		}
	}
	// in-memory databases don't have a WAL - we estimate how large their changeset is based on the undo properties
	if (!db.IsSystem()) {
		auto &storage_manager = db.GetStorageManager();
		if (storage_manager.InMemory() || db.GetRecoveryMode() == RecoveryMode::NO_WAL_WRITES) {
			storage_manager.AddWALSize(undo_properties.estimated_size);
		}
	}
	// obtain a commit id for the transaction
	CommitInfo info;
	info.commit_id = GetCommitTimestamp();

	// commit the UndoBuffer of the transaction
	if (!error.HasError()) {
		if (HasOtherTransactions(transaction)) {
			info.active_transactions = ActiveTransactionState::OTHER_TRANSACTIONS;
		} else {
			info.active_transactions = ActiveTransactionState::NO_OTHER_TRANSACTIONS;
		}
		error = transaction.Commit(db, info, std::move(commit_state));
	}

	if (error.HasError()) {
		DUCKDB_LOG(context, TransactionLogType, db, "Rollback (after failed commit)", info.commit_id);

		// COMMIT not successful: ROLLBACK.
		checkpoint_decision = CheckpointDecision(error.Message());
		transaction.commit_id = 0;

		auto rollback_error = transaction.Rollback();
		if (rollback_error.HasError()) {
			throw FatalException(
			    "Failed to rollback transaction. Cannot continue operation.\nOriginal Error: %s\nRollback Error: %s",
			    error.Message(), rollback_error.Message());
		}
	} else {
		DUCKDB_LOG(context, TransactionLogType, db, "Commit", info.commit_id);
		last_commit = info.commit_id;

		// check if catalog changes were made
		if (transaction.catalog_version >= TRANSACTION_ID_START) {
			transaction.catalog_version = ++last_committed_version;
		}
		if (transaction.ChangesMade() &&
		    (info.wal_flush_offset > 0 || (skip_wal_write_due_to_checkpoint && checkpoint_decision.can_checkpoint))) {
			// visible now but not yet durable (WAL commits fsync in GroupSync below; checkpoint-instead-of-WAL commits
			// become durable when their checkpoint completes). Record it under the transaction+WAL locks (monotonic
			// store) so new snapshots stay bounded below it until the durability point raises last_durable_commit.
			// In-memory/temporary commits are durable at publish, so they are not recorded and never defer snapshots
			prior_pending = last_pending_commit.load(std::memory_order_relaxed);
			last_pending_commit.store(info.commit_id, std::memory_order_release);
		}
	}
	OnCommitCheckpointDecision(checkpoint_decision, transaction);

	if (!checkpoint_decision.can_checkpoint && lock) {
		// we won't checkpoint after all due to an error during commit: unlock the checkpoint lock again
		skip_wal_write_due_to_checkpoint = false;
		lock.reset();
		gate_holder.Release();
	}

	// commit successful: remove the transaction id from the list of active transactions
	// potentially resulting in garbage collection
	bool store_transaction = undo_properties.has_updates || undo_properties.has_index_deletes ||
	                         undo_properties.has_catalog_changes || error.HasError();

	// Remove the transaction from the list of active transactions and gather cleanup information.
	auto cleanup_info = RemoveTransaction(transaction, store_transaction);
	if (cleanup_info->ScheduleCleanup()) {
		lock_guard<mutex> q_lock(cleanup_queue_lock);
		cleanup_queue.emplace(std::move(cleanup_info));
	}

	// We do not need to hold the transaction lock during cleanup of transactions,
	// as they (1) have been removed, or (2) enter cleanup_info.
	t_lock.unlock();
	// if we have skipped the WAL write due to checkpoint, we keep the WAL lock while checkpointing
	// this prevents any concurrent transactions from happening during this time
	if (!skip_wal_write_due_to_checkpoint && held_wal_lock.owns_lock()) {
		held_wal_lock.unlock();
	}

	// raise the durable horizon to this commit so new snapshots include it. Acknowledgements can race out of
	// commit order (a later commit's fsync may cover an earlier one still parked), hence the raise-only max
	auto raise_durable_horizon = [&]() {
		transaction_t durable = last_durable_commit.load(std::memory_order_relaxed);
		while (durable < info.commit_id &&
		       !last_durable_commit.compare_exchange_weak(durable, info.commit_id, std::memory_order_release,
		                                                  std::memory_order_relaxed)) {
		}
	};
	// group commit: the entries + WAL_FLUSH marker are already in the page cache (deferred fsync); issue the grouped
	// fsync here with no lock held, so concurrent committers overlap or share fsyncs, and block until it covers this
	// commit's bytes -- durable before acknowledged. WAL append order equals commit order, so a later committer's
	// fsync also covers this one, preserving prefix-consistent replay
	if (!error.HasError() && info.wal_flush_offset > 0) {
		wal_ref->GroupSync(info.wal_flush_offset);
		raise_durable_horizon();
	}

	CleanupTransactions();

	// now perform a checkpoint if (1) we are able to checkpoint, and (2) the WAL has reached sufficient size to
	// checkpoint
	if (checkpoint_decision.can_checkpoint) {
		if (!lock || lock->GetType() != StorageLockType::EXCLUSIVE) {
			throw InternalException("Checkpointing requires an exclusive lock to be held");
		}
		// we can unlock the transaction lock while checkpointing
		// checkpoint the database to disk
		CheckpointOptions options;
		options.action = CheckpointAction::ALWAYS_CHECKPOINT;
		options.type = checkpoint_decision.type;
		options.wal_lock = held_wal_lock.owns_lock() ? &held_wal_lock : nullptr;
		auto &storage_manager = db.GetStorageManager();
		if (options.type != CheckpointType::CONCURRENT_CHECKPOINT) {
			// a vacuuming checkpoint frees segments not-yet-cleaned undo still references, so wait for earlier
			// commits' fsyncs and run the held-back cleanups (including this commit's own). Safe: a WAL-skipping
			// commit holds the start gate, so no snapshot bounded below its still-pending undo exists or can start
			while (last_durable_commit.load(std::memory_order_acquire) < prior_pending) {
				std::this_thread::yield();
			}
			DrainCleanups(start_gate.owns_lock());
			if (options.type == CheckpointType::FULL_CHECKPOINT && !start_gate.owns_lock() &&
			    GetLastCommit() > LowestActiveStart()) {
				// a snapshot that started during this commit's fsync window is bounded below it and still needs
				// the versions a full checkpoint would vacuum: fall back to a concurrent checkpoint, the same rule
				// Checkpoint() applies. Race-free: the exclusive checkpoint lock blocks new write commits, so no
				// new bounded snapshot can appear after this check
				options.type = CheckpointType::CONCURRENT_CHECKPOINT;
			}
		}
		try {
			storage_manager.CreateCheckpoint(context, options);
			// a checkpoint-instead-of-WAL commit is durable once its checkpoint completes: raise the horizon so new
			// snapshots include it (no-op for WAL-written commits, whose GroupSync above already raised it)
			if (!error.HasError()) {
				raise_durable_horizon();
			}
		} catch (std::exception &ex) {
			if (wal_written) {
				context.transaction.SetAutocheckpointError(BuildAutocheckpointError(db, ex));
			} else {
				error.Merge(ErrorData(ex));
				// the checkpoint that was to carry this commit failed: the changes were not persisted and the
				// commit is reported as failed, but they remain visible in memory (as upstream leaves them).
				// Raise the horizon so this commit cannot jam the pending window and durability waits forever
				raise_durable_horizon();
			}
		}
	}

	return error;
}

void DuckTransactionManager::RollbackTransaction(Transaction &transaction_p) {
	auto &transaction = transaction_p.Cast<DuckTransaction>();

	DUCKDB_LOG(db.GetDatabase(), TransactionLogType, db, "Rollback", transaction.transaction_id);

	ErrorData error;
	{
		// Obtain the transaction lock and roll back.
		lock_guard<mutex> t_lock(transaction_lock);
		error = transaction.Rollback();

		// Remove the transaction from the list of active transactions and gather cleanup information.
		auto cleanup_info = RemoveTransaction(transaction);
		if (cleanup_info->ScheduleCleanup()) {
			lock_guard<mutex> q_lock(cleanup_queue_lock);
			cleanup_queue.emplace(std::move(cleanup_info));
		}
	}

	CleanupTransactions();

	if (error.HasError()) {
		throw FatalException("Failed to rollback transaction. Cannot continue operation.\nError: %s", error.Message());
	}
}

unique_ptr<DuckCleanupInfo> DuckTransactionManager::RemoveTransaction(DuckTransaction &transaction) noexcept {
	return RemoveTransaction(transaction, transaction.ChangesMade());
}

unique_ptr<DuckCleanupInfo> DuckTransactionManager::RemoveTransaction(DuckTransaction &transaction,
                                                                      bool store_transaction) noexcept {
	auto cleanup_info = make_uniq<DuckCleanupInfo>();

	// Find the transaction in the active transactions,
	// as well as the lowest start time, transaction id, and active query.
	idx_t t_index = active_transactions.size();
	auto lowest_start_time = TRANSACTION_ID_START;
	auto lowest_transaction_id = MAX_TRANSACTION_ID;
	for (idx_t i = 0; i < active_transactions.size(); i++) {
		if (active_transactions[i].get() == &transaction) {
			t_index = i;
			continue;
		}
		lowest_start_time = MinValue(lowest_start_time, active_transactions[i]->start_time);
		lowest_transaction_id = MinValue(lowest_transaction_id, active_transactions[i]->transaction_id);
	}
	// while commits are awaiting their group fsync, a future snapshot can be bounded down to just above the durable
	// horizon: clamp the cleanup threshold there, so versions such a snapshot must still see are not destroyed
	lowest_start_time = MinValue(lowest_start_time, DurableSnapshotBound(TRANSACTION_ID_START));
	lowest_active_start = lowest_start_time;
	lowest_active_id = lowest_transaction_id;
	D_ASSERT(t_index != active_transactions.size());

	// Decide if we need to store the transaction, or if we can schedule it for cleanup.
	auto current_transaction = std::move(active_transactions[t_index]);
	if (store_transaction) {
		// If the transaction made any changes, we need to keep it around.
		if (transaction.commit_id != 0) {
			// The transaction was committed.
			// We add it to the list of recently committed transactions.
			recently_committed_transactions.push_back(std::move(current_transaction));
		} else {
			// The transaction was aborted.
			cleanup_info->transactions.push_back(std::move(current_transaction));
		}
	} else if (transaction.ChangesMade()) {
		// We do not need to store the transaction, directly schedule it for cleanup.
		current_transaction->awaiting_cleanup = true;
		cleanup_info->transactions.push_back(std::move(current_transaction));
	}
	cleanup_info->lowest_start_time = lowest_start_time;

	// Remove the transaction from the list of active transactions.
	active_transactions.unsafe_erase_at(t_index);

	// Traverse the recently_committed transactions to see if we can move any
	// to the list of transactions awaiting GC.
	idx_t i = 0;
	for (; i < recently_committed_transactions.size(); i++) {
		D_ASSERT(recently_committed_transactions[i]);
		if (recently_committed_transactions[i]->commit_id >= lowest_start_time) {
			// recently_committed_transactions is ordered on commit_id.
			// Thus, if the current commit_id is greater than
			// lowest_start_time, any subsequent commit IDs are also greater.
			break;
		}

		recently_committed_transactions[i]->awaiting_cleanup = true;
		cleanup_info->transactions.push_back(std::move(recently_committed_transactions[i]));
	}

	if (i > 0) {
		// We moved these transactions to the list of transactions awaiting GC.
		auto start = recently_committed_transactions.begin();
		auto end = recently_committed_transactions.begin() + static_cast<int64_t>(i);
		recently_committed_transactions.erase(start, end);
	}

	return cleanup_info;
}

idx_t DuckTransactionManager::GetCatalogVersion(Transaction &transaction_p) {
	auto &transaction = transaction_p.Cast<DuckTransaction>();
	return transaction.catalog_version;
}

void DuckTransactionManager::PushCatalogEntry(Transaction &transaction_p, duckdb::CatalogEntry &entry,
                                              duckdb::data_ptr_t extra_data, duckdb::idx_t extra_data_size) {
	auto &transaction = transaction_p.Cast<DuckTransaction>();
	if (!db.IsSystem() && !db.IsTemporary() && transaction.IsReadOnly()) {
		throw InternalException("Attempting to do catalog changes on a transaction that is read-only - "
		                        "this should not be possible");
	}
	transaction.catalog_version = ++last_uncommitted_catalog_version;
	transaction.PushCatalogEntry(entry, extra_data, extra_data_size);
}

void DuckTransactionManager::PushAttach(Transaction &transaction_p, AttachedDatabase &attached_db) {
	auto &transaction = transaction_p.Cast<DuckTransaction>();
	if (!db.IsSystem()) {
		throw InternalException("Can only ATTACH in the system catalog");
	}
	transaction.catalog_version = ++last_uncommitted_catalog_version;
	transaction.PushAttach(attached_db);
}

} // namespace duckdb
