#include "duckdb/transaction/duck_transaction_manager.hpp"

#include <chrono>
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
#include "duckdb/transaction/transaction_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/checkpoint/checkpoint_options.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static ErrorData BuildAutocheckpointError(AttachedDatabase &db, const std::exception &ex) {
	ErrorData original(ex);
	string recovery = db.IsInitialDatabase() ? "Reopen the database instance to recover."
	                                         : "Detach and reattach the database to recover.";
	string msg = StringUtil::Format("Transaction COMMIT succeeded and is durable, but the autocheckpoint failed. %s %s",
	                                recovery, original.RawMessage());
	return ErrorData(original.Type(), msg);
}

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
	// real transactions start above the system transaction's timestamp
	current_start_timestamp = SYSTEM_TRANSACTION_TIMESTAMP + 1;
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
	if (!meta_transaction.IsReadOnly()) {
		start_lock.lock();
	}
	lock_guard<mutex> lock(transaction_lock);
	if (current_start_timestamp >= TRANSACTION_ID_START) { // LCOV_EXCL_START
		throw InternalException("Cannot start more transactions, ran out of "
		                        "transaction identifiers!");
	} // LCOV_EXCL_STOP

	// obtain the start time and transaction ID of this transaction
	transaction_t unique_start_time = current_start_timestamp++;
	transaction_t transaction_id = current_transaction_id++;
	// snapshots must not observe commits that are not yet durable, nor a newer catalog version
	auto snapshot_bound = GetSnapshotBound();
	auto start_time = MinValue<transaction_t>(unique_start_time, snapshot_bound.start_time);
	auto catalog_version = MinValue<idx_t>(last_committed_version, snapshot_bound.catalog_version);
	if (active_transactions.empty()) {
		lowest_active_start = start_time;
		lowest_active_id = transaction_id;
	}

	// create the actual transaction
	auto transaction =
	    make_uniq<DuckTransaction>(*this, context, start_time, unique_start_time, transaction_id, catalog_version);
	auto &transaction_ref = *transaction;

	// store it in the set of active transactions
	active_transactions.push_back(std::move(transaction));
	return transaction_ref;
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
	if (HasUnsyncedCommits()) {
		// commits pending durability behave like active transactions (see UnsyncedCommit)
		return true;
	}
	for (auto &active_transaction : active_transactions) {
		if (!RefersToSameObject(*active_transaction, transaction)) {
			return true;
		}
	}
	return false;
}

DuckTransactionManager::CheckpointDecision
DuckTransactionManager::CanCheckpoint(DuckTransaction &transaction, unique_ptr<StorageLockKey> &lock,
                                      const UndoBufferProperties &undo_properties) {
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
	// try to lock the checkpoint lock
	lock = transaction.TryGetCheckpointLock();
	if (!lock) {
		return CheckpointDecision("Failed to obtain checkpoint lock - another thread is writing/checkpointing or "
		                          "another read transaction relies on data that is not yet committed");
	}
	return CheckpointDecision(CheckpointType::FULL_CHECKPOINT);
}

DuckTransactionManager::CheckpointDecision
DuckTransactionManager::GetCheckpointType(DuckTransaction &transaction, const UndoBufferProperties &undo_properties) {
	auto &storage_manager = db.GetStorageManager();
	auto checkpoint_type = CheckpointType::FULL_CHECKPOINT;
	bool has_other_transactions = HasOtherTransactions(transaction);
	if (has_other_transactions) {
		if (undo_properties.has_updates || undo_properties.has_dropped_entries) {
			// other transactions - or snapshots bounded below pending commits - may need older data
			string other_transactions;
			for (auto &active_transaction : active_transactions) {
				if (!RefersToSameObject(*active_transaction, transaction)) {
					if (!other_transactions.empty()) {
						other_transactions += ", ";
					}
					other_transactions += "[" + to_string(active_transaction->transaction_id) + "]";
				}
			}
			if (other_transactions.empty()) {
				other_transactions = "[commits pending durability]";
			}
			if (undo_properties.has_dropped_entries) {
				return CheckpointDecision("Transaction has dropped catalog entries and there are other transactions "
				                          "active\nActive transactions: " +
				                          other_transactions);
			}
			return CheckpointDecision(
			    "Transaction has performed updates and there are other transactions active\nActive transactions: " +
			    other_transactions);
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
	// drain pending commits here, where the query is still cancellable: inside the checkpoint an
	// exception would invalidate the database. The authoritative drain under the WAL lock follows
	if (!db.IsSystem() && db.HasStorageManager() && !db.GetStorageManager().InMemory()) {
		WaitForDurability(context);
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
			lock = checkpoint_lock.TryGetExclusiveLock();
		}
	}
	CheckpointOptions options;
	if (!VisibleToSnapshot(GetLastCommit(), LowestActiveStart())) {
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

bool DuckTransactionManager::HasUnsyncedCommits() {
	lock_guard<mutex> guard(durability_lock);
	return !unsynced_commits.empty();
}

DuckTransactionManager::SnapshotBound DuckTransactionManager::GetSnapshotBound() {
	lock_guard<mutex> guard(durability_lock);
	SnapshotBound bound;
	for (auto &entry : unsynced_commits) {
		if (entry.commit_id <= durable_commit_bound) {
			// durable already - its own thread has not removed the entry yet
			continue;
		}
		// the first commit that is not durable: a snapshot stops below it, and the catalog version
		// recorded just before it published is exactly the one that snapshot observes
		bound.start_time = entry.commit_id;
		bound.catalog_version = entry.catalog_version;
		break;
	}
	return bound;
}

void DuckTransactionManager::RegisterUnsyncedCommit(transaction_t commit_id, idx_t wal_offset, idx_t catalog_version) {
	lock_guard<mutex> guard(durability_lock);
	if (unsynced_commits.empty()) {
		// nothing pending: everything below this commit is durable
		durable_commit_bound = commit_id - 1;
	}
	// the front entry may sit at or below the bound, but a new commit always registers above it
	D_ASSERT(wal_offset > 0);
	D_ASSERT(commit_id > durable_commit_bound);
	D_ASSERT(unsynced_commits.empty() || unsynced_commits.back().wal_offset <= wal_offset);
	unsynced_commits.push_back(UnsyncedCommit {commit_id, wal_offset, catalog_version});
}

bool DuckTransactionManager::FinishCommitDurability(transaction_t commit_id, idx_t synced_offset) {
	unique_lock<mutex> guard(durability_lock);
	// advance over every commit the sync covered, including ones whose threads have not woken up
	// yet, so that an ack implies observability; then drop this thread's entry
	auto new_bound = durable_commit_bound;
	for (auto it = unsynced_commits.begin(); it != unsynced_commits.end(); it++) {
		if (it->wal_offset > synced_offset) {
			break;
		}
		new_bound = MaxValue<transaction_t>(new_bound, it->commit_id);
		if (it->commit_id == commit_id) {
			unsynced_commits.erase(it);
			break;
		}
	}
	bool advanced = new_bound > durable_commit_bound;
	durable_commit_bound = new_bound;
#ifdef DEBUG
	// offsets are registered in flush order, so the walk always reaches this thread's entry
	for (auto &entry : unsynced_commits) {
		D_ASSERT(entry.commit_id != commit_id);
	}
#endif
	if (unsynced_commits.empty()) {
		// notify without holding the lock, so waiters do not wake up into a held mutex
		guard.unlock();
		durability_cv.notify_all();
	}
	return advanced;
}

DuckTransactionManager::TransactionHorizon
DuckTransactionManager::UpdateTransactionHorizon(optional_ptr<DuckTransaction> exclude) {
	TransactionHorizon horizon;
	for (auto &active_transaction : active_transactions) {
		if (exclude && active_transaction.get() == exclude.get()) {
			continue;
		}
		horizon.lowest_start_time = MinValue(horizon.lowest_start_time, active_transaction->start_time);
		horizon.lowest_transaction_id = MinValue(horizon.lowest_transaction_id, active_transaction->transaction_id);
	}
	// commits pending durability pin the horizon: bounded snapshots may need their old versions
	horizon.lowest_start_time = MinValue<transaction_t>(horizon.lowest_start_time, GetSnapshotBound().start_time);
	lowest_active_start = horizon.lowest_start_time;
	lowest_active_id = horizon.lowest_transaction_id;
	return horizon;
}

void DuckTransactionManager::SweepCommittedTransactions(transaction_t lowest_start_time,
                                                        DuckCleanupInfo &cleanup_info) {
	idx_t i = 0;
	for (; i < recently_committed_transactions.size(); i++) {
		D_ASSERT(recently_committed_transactions[i]);
		if (!VisibleToSnapshot(recently_committed_transactions[i]->commit_id, lowest_start_time)) {
			// recently_committed_transactions is ordered on commit_id.
			// Thus, if the current commit_id is greater than
			// lowest_start_time, any subsequent commit IDs are also greater.
			break;
		}
		recently_committed_transactions[i]->awaiting_cleanup = true;
		cleanup_info.transactions.push_back(std::move(recently_committed_transactions[i]));
	}
	if (i > 0) {
		// We moved these transactions to the list of transactions awaiting GC.
		recently_committed_transactions.erase(recently_committed_transactions.begin(),
		                                      recently_committed_transactions.begin() + static_cast<int64_t>(i));
	}
}

void DuckTransactionManager::GarbageCollectDurableTransactions() {
	auto cleanup_info = make_uniq<DuckCleanupInfo>();
	{
		// composed and queued under the transaction lock, as cleanups must run in commit order
		lock_guard<mutex> t_lock(transaction_lock);
		auto horizon = UpdateTransactionHorizon(nullptr);
		cleanup_info->lowest_start_time = horizon.lowest_start_time;
		SweepCommittedTransactions(horizon.lowest_start_time, *cleanup_info);
		QueueCleanup(std::move(cleanup_info));
	}
}

void DuckTransactionManager::QueueCleanup(unique_ptr<DuckCleanupInfo> cleanup_info) {
	if (!cleanup_info->ScheduleCleanup()) {
		return;
	}
	lock_guard<mutex> q_lock(cleanup_queue_lock);
	cleanup_queue.emplace(std::move(cleanup_info));
}

void DuckTransactionManager::MarkDurabilityFailed() {
	{
		lock_guard<mutex> guard(durability_lock);
		durability_failed = true;
	}
	durability_cv.notify_all();
}

void DuckTransactionManager::WaitForDurability(optional_ptr<ClientContext> context) {
	unique_lock<mutex> guard(durability_lock);
	auto drained = [&]() {
		return unsynced_commits.empty() || durability_failed;
	};
	while (!drained()) {
		if (!context) {
			// nothing to cancel (shutdown, or inside a checkpoint where throwing would invalidate)
			durability_cv.wait(guard);
			continue;
		}
		// this waits on fsyncs issued by other connections, so poll rather than block outright
		if (!durability_cv.wait_for(guard, std::chrono::milliseconds(10), drained)) {
			guard.unlock();
			context->InterruptCheck();
			guard.lock();
		}
	}
	if (durability_failed && !unsynced_commits.empty()) {
		throw IOException("Cannot wait for WAL durability: a WAL sync has failed");
	}
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
	unique_lock<mutex> t_lock(transaction_lock);
	if (!db.IsSystem() && !db.IsTemporary()) {
		if (transaction.ChangesMade()) {
			if (transaction.IsReadOnly()) {
				throw InternalException("Attempting to commit a transaction that is read-only but has made changes - "
				                        "this should not be possible");
			}
		}
	}

	// check if we can checkpoint
	unique_ptr<StorageLockKey> lock;
	auto undo_properties = transaction.GetUndoProperties();
	auto checkpoint_decision = CanCheckpoint(transaction, lock, undo_properties);
	ErrorData error;
	unique_lock<mutex> held_wal_lock;
	unique_ptr<StorageCommitState> commit_state;
	optional_ptr<WriteAheadLog> commit_wal;
	bool commit_registered = false;
	bool skip_wal_write_due_to_checkpoint = false;
	bool wal_written = false;
	if (checkpoint_decision.can_checkpoint) {
		// we can perform an automatic checkpoint
		// we have two options:
		// either we write to the WAL, in which case we can perform concurrent commits while running
		// OR we skip writing to the WAL, in which case we cannot perform concurrent commits
		// the reason for this is that if we don't write this transactions' changes to the WAL
		// any failure during checkpoint will cause this transactions' changes to be lost,
		// while later concurrent commits will not be
		// this can cause undefined state, as those commits were made assuming this one was already committed
		if (undo_properties.estimated_size >= Settings::Get<AutoCheckpointSkipWalThresholdSetting>(context)) {
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
		if (HasOtherTransactions(transaction) || wal_written) {
			// bounded snapshots can still start below a WAL-written commit and need its old state
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
		if (wal_written && info.wal_sync_offset > 0) {
			// published but not yet durable: track it until the sync below. No flush marker
			// (offset 0) means nothing reached the WAL, so there is nothing to wait for
			commit_wal = db.GetStorageManager().GetWAL();
			if (commit_wal) {
				// registration precedes this commit's own catalog-version bump below
				RegisterUnsyncedCommit(info.commit_id, info.wal_sync_offset, last_committed_version);
				commit_registered = true;
			}
		}

		// check if catalog changes were made
		if (transaction.catalog_version >= TRANSACTION_ID_START) {
			transaction.catalog_version = ++last_committed_version;
		}
	}
	try {
		OnCommitCheckpointDecision(checkpoint_decision, transaction);

		if (!checkpoint_decision.can_checkpoint && lock) {
			// we won't checkpoint after all due to an error during commit: unlock the checkpoint lock again
			skip_wal_write_due_to_checkpoint = false;
			lock.reset();
		}

		// commit successful: remove the transaction id from the list of active transactions
		// potentially resulting in garbage collection
		bool store_transaction = undo_properties.has_updates || undo_properties.has_index_deletes ||
		                         undo_properties.has_catalog_changes || error.HasError();

		// Remove the transaction from the list of active transactions and gather cleanup information.
		QueueCleanup(RemoveTransaction(transaction, store_transaction));
	} catch (...) {
		if (commit_registered) {
			// the registered commit can never be finished: its entry would pin the snapshot bound
			// and hang every checkpoint, so invalidate rather than leave a healthy-looking database
			MarkDurabilityFailed();
			ValidChecker::Invalidate(db,
			                         "Failed to finish committing a transaction whose WAL write is not yet durable");
		}
		throw;
	}

	// We do not need to hold the transaction lock during cleanup of transactions,
	// as they (1) have been removed, or (2) enter cleanup_info.
	t_lock.unlock();
	// if we have skipped the WAL write due to checkpoint, we keep the WAL lock while checkpointing
	// this prevents any concurrent transactions from happening during this time
	if (!skip_wal_write_due_to_checkpoint && held_wal_lock.owns_lock()) {
		held_wal_lock.unlock();
	}

	if (commit_registered) {
		// make the commit durable before acknowledging it; one fsync can cover many commits. The
		// WAL pointer stays valid without the lock: checkpoints destroy it only after draining
		D_ASSERT(!error.HasError());
		try {
			commit_wal->SyncUpTo(info.wal_sync_offset);
			if (FinishCommitDurability(info.commit_id, info.wal_sync_offset)) {
				// the bound advanced: sweep the transactions it was pinning
				GarbageCollectDurableTransactions();
			}
		} catch (std::exception &ex) {
			// published and no longer revertable, but not durable: poison first so drains fail and
			// release the WAL lock, then drop the never-durable tail so a restart cannot replay it
			error = ErrorData(ex);
			MarkDurabilityFailed();
			try {
				auto wal_guard = db.GetStorageManager().GetWALLock();
				commit_wal->TruncateUnsyncedTail();
			} catch (...) { // NOLINT: the database is being invalidated regardless
			}
			ValidChecker::Invalidate(db, "Failed to sync the WAL after committing: " + error.Message());
		} catch (...) {
			// as above - nothing may escape leaving the commit registered but unfinished
			MarkDurabilityFailed();
			ValidChecker::Invalidate(db, "Failed to sync the WAL after committing (unknown error)");
			throw;
		}
	}

	CleanupTransactions();

	if (checkpoint_decision.can_checkpoint && (undo_properties.has_updates || undo_properties.has_dropped_entries) &&
	    !VisibleToSnapshot(GetLastCommit(), LowestActiveStart())) {
		// a snapshot bounded below this commit started during the sync window and still needs the
		// pre-commit state - skip the checkpoint, as GetCheckpointType would have. The skip-wal
		// case holds the WAL lock and registers nothing, so no bounded snapshot can exist there
		D_ASSERT(!skip_wal_write_due_to_checkpoint);
		checkpoint_decision = CheckpointDecision("snapshots bounded below this commit need its pre-commit state");
		lock.reset();
	}

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
		try {
			storage_manager.CreateCheckpoint(context, options);
		} catch (std::exception &ex) {
			if (wal_written) {
				context.transaction.SetAutocheckpointError(BuildAutocheckpointError(db, ex));
			} else {
				error.Merge(ErrorData(ex));
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
		QueueCleanup(RemoveTransaction(transaction));
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

	// Find the transaction in the active transactions and recompute the horizon without it.
	idx_t t_index = active_transactions.size();
	for (idx_t i = 0; i < active_transactions.size(); i++) {
		if (active_transactions[i].get() == &transaction) {
			t_index = i;
			break;
		}
	}
	D_ASSERT(t_index != active_transactions.size());
	auto horizon = UpdateTransactionHorizon(&transaction);
	auto lowest_start_time = horizon.lowest_start_time;

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
	SweepCommittedTransactions(lowest_start_time, *cleanup_info);

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
