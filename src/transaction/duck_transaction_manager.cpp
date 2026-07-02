#include "duckdb/transaction/duck_transaction_manager.hpp"

#include "duckdb/main/client_data.hpp"

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/checkpoint/checkpoint_options.hpp"
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
	if (!meta_transaction.IsReadOnly()) {
		start_lock.lock();
	}
	lock_guard<mutex> lock(transaction_lock);
	if (current_start_timestamp >= TRANSACTION_ID_START) { // LCOV_EXCL_START
		throw InternalException("Cannot start more transactions, ran out of "
		                        "transaction identifiers!");
	} // LCOV_EXCL_STOP

	// obtain the start time and transaction ID of this transaction
	transaction_t start_time = current_start_timestamp++;
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

void DuckTransactionManager::RetireAfterCheckpoint(shared_ptr<void> retired_object) {
	if (!retired_object) {
		return;
	}
	// Capture the start-timestamp horizon: current_start_timestamp is the next start time / commit id to be handed
	// out, so every transaction that currently exists (active or already committed) has a start_time / commit_id
	// strictly below it. Once lowest_active_start reaches this epoch, none of those transactions is active anymore
	// and their undo has been cleaned, so nothing can reference the retired object's UpdateSegments.
	transaction_t epoch;
	{
		lock_guard<mutex> t_lock(transaction_lock);
		epoch = current_start_timestamp;
	}
	lock_guard<mutex> l(retired_lock);
	retired_after_checkpoint.emplace_back(epoch, std::move(retired_object));
}

void DuckTransactionManager::CleanupTransactions() {
	lock_guard<mutex> c_lock(cleanup_lock);
	while (true) {
		unique_ptr<DuckCleanupInfo> top_cleanup_info;
		{
			lock_guard<mutex> q_lock(cleanup_queue_lock);
			if (cleanup_queue.empty()) {
				// all transactions have been cleaned up - done
				break;
			}
			top_cleanup_info = std::move(cleanup_queue.front());
			cleanup_queue.pop();
		}
		if (top_cleanup_info) {
			top_cleanup_info->Cleanup();
		}
	}
	// The cleanup queue is now drained. Free every retired object whose epoch the horizon has reached: once
	// lowest_active_start >= epoch, no transaction that existed when the object was retired is still active, so its
	// committed undo has been cleaned above and no UpdateInfo can reference the object's UpdateSegments anymore. When
	// the database is quiescent lowest_active_start is TRANSACTION_ID_START (its idle max), which frees everything.
	auto horizon = lowest_active_start.load();
	vector<shared_ptr<void>> to_free;
	{
		lock_guard<mutex> l(retired_lock);
		for (idx_t i = 0; i < retired_after_checkpoint.size();) {
			if (retired_after_checkpoint[i].first <= horizon) {
				to_free.push_back(std::move(retired_after_checkpoint[i].second));
				retired_after_checkpoint[i] = std::move(retired_after_checkpoint.back());
				retired_after_checkpoint.pop_back();
			} else {
				i++;
			}
		}
	}
	// drop the references outside the lock
}

void DuckTransactionManager::RegisterPendingCommit(idx_t publish_seq) {
	lock_guard<mutex> guard(publish_lock);
	pending_commit_publishes.insert(publish_seq);
}

void DuckTransactionManager::WaitForPublishTurn(idx_t publish_seq) {
	std::unique_lock<mutex> guard(publish_lock);
	publish_cv.wait(guard, [&]() {
		D_ASSERT(!pending_commit_publishes.empty());
		return *pending_commit_publishes.begin() == publish_seq;
	});
}

void DuckTransactionManager::FinishPendingCommit(idx_t publish_seq) {
	{
		lock_guard<mutex> guard(publish_lock);
		pending_commit_publishes.erase(publish_seq);
	}
	publish_cv.notify_all();
}

unique_ptr<StorageLockKey> DuckTransactionManager::BlockPendingCommits(optional_ptr<DataTableInfo> table_info) {
	// Nothing to gate when this is not a table DDL (table_info is null), on the system catalog, or on an in-memory
	// database - none of those have group commits (DDL on them, e.g. registering built-in functions at load, must
	// not touch a gate that does not apply).
	if (!table_info || db.IsSystem() || db.GetStorageManager().InMemory()) {
		return nullptr;
	}
	// Take this table's publish gate exclusively: this waits for all in-flight commits on the table (which hold it
	// SHARED from before their catalog validation through publish) to finish and blocks new ones until the returned
	// handle is released, so the caller can mutate the table's catalog entry / index list without a commit's
	// validation going stale or its publish racing the index-list change. Scoped to the table, so commits to other
	// tables are unaffected. Writer priority in StorageLock prevents a steady stream of commits from starving this.
	return table_info->GetPublishGateExclusive();
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
	// The WAL lock is a read-write lock: data-only group commits take it SHARED (held through publish, so concurrent
	// committers batch and a checkpoint/catalog change taking it EXCLUSIVE waits for them); checkpointing and
	// catalog-changing commits take it EXCLUSIVE.
	unique_ptr<StorageLockKey> held_wal_lock;
	// Held by shared (data) committers across their WAL entry+marker append so their entries stay contiguous; the
	// shared WAL lock alone would let concurrent committers interleave their entries and corrupt the WAL. Released
	// before the fsync so batching is preserved. Exclusive committers do not need it (the WAL lock serializes them).
	unique_lock<mutex> append_guard;
	// Per-table publish gates: a commit holds each modified table's gate SHARED from before its catalog validation
	// (in WriteToWAL) through publish, so a concurrent DDL on that table (which takes the gate EXCLUSIVE via
	// BlockPendingCommits) cannot make the validation stale or race the publish's index updates. Acquired in
	// address order (see GetModifiedTableInfos) for deadlock-freedom; released after publish. modified_table_infos
	// keeps the DataTableInfos alive while their gate handles are held.
	vector<shared_ptr<DataTableInfo>> modified_table_infos;
	vector<unique_ptr<StorageLockKey>> table_gates;
	unique_ptr<StorageCommitState> commit_state;
	bool skip_wal_write_due_to_checkpoint = false;
	bool wal_written = false;
	// WAL offset our flush marker requires for durability (set by CommitToWAL, used as the deferred SyncUpTo target)
	idx_t flush_marker_offset = 0;
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
	// Catalog-changing commits cannot defer publishing their changes (see below) - they publish while holding the
	// transaction lock. A DDL's catalog-version attachment is kept from interleaving a data commit's validation and
	// publish by the per-table publish gate (see BlockPendingCommits / table_gates), not by this commit path.
	bool has_catalog_changes = undo_properties.has_catalog_changes;
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
		// grab the WAL lock and hold it until the entire commit is finished. Checkpointing or catalog-changing
		// commits take it EXCLUSIVE (so they serialize against all data writers); pure data commits take it SHARED
		// (so they run concurrently and batch their fsyncs).
		bool need_exclusive_wal = has_catalog_changes || checkpoint_decision.can_checkpoint;
		held_wal_lock = need_exclusive_wal ? storage_manager.GetWALLockExclusive() : storage_manager.GetWALLockShared();
		if (!need_exclusive_wal) {
			// shared WAL holders can run concurrently, so serialize their entry+marker appends to keep each
			// transaction's WAL entries contiguous (released after the marker, before the fsync)
			append_guard = storage_manager.GetWALAppendLock();
		}
		// take each modified table's publish gate SHARED before validating/writing, held through publish
		modified_table_infos = transaction.GetModifiedTableInfos();
		for (auto &table_info : modified_table_infos) {
			table_gates.push_back(table_info->GetPublishGateShared());
		}

		// Commit the changes to the WAL.
		if (!skip_wal_write_due_to_checkpoint) {
			error = transaction.WriteToWAL(context, db, commit_state);
			wal_written = true;
			// no drain needed: catalog-changing commits hold the WAL lock EXCLUSIVELY, so no deferred data commit
			// can be between its flush marker and its publish
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
			D_ASSERT(held_wal_lock);
			// unlock the transaction lock while we are writing to the WAL
			t_lock.unlock();
			error = transaction.WriteToWAL(context, db, commit_state);
			wal_written = true;
			// held_wal_lock is EXCLUSIVE here (we took it for the checkpoint), so no drain is needed
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
	// The commit timestamp is assigned later, at publish time (not here): a transaction that starts while this
	// commit is still being made durable must not observe a commit id that is not yet published, otherwise it
	// could read pre-commit data without the update conflict being detected. The deferred path assigns it under
	// the transaction lock right before publishing; the synchronous path assigns it just before committing below.
	CommitInfo info;

	// Group commit: data-only commits that write to the WAL defer publishing
	// (making their changes visible) until after their WAL entries have been fsynced. This way no transaction can
	// ever observe a commit that is not yet durable, while the fsync itself happens outside of the transaction
	// lock and the WAL lock, so that concurrently committing transactions can share a single fsync.
	// Catalog-changing commits take the non-deferred path: they publish under the transaction lock and fsync inline
	// before releasing it - no new transaction can start in the meantime, so those commits are also never visible
	// before they are durable.
	// Only data-only, non-checkpointing commits defer: they hold the WAL lock SHARED, so the deferred publish
	// turnstile (which needs concurrent committers to make progress) cannot deadlock. Checkpointing and
	// catalog-changing commits hold the WAL lock EXCLUSIVELY and publish synchronously.
	bool defer_publish =
	    !error.HasError() && commit_state && !has_catalog_changes && !checkpoint_decision.can_checkpoint;
	// the publish sequence orders deferred publishes in WAL order; it is independent of the commit timestamp
	idx_t publish_seq = 0;
	if (defer_publish) {
		// assign the publish sequence and register for in-order publishing. A concurrent catalog change on a table we
		// modified cannot interleave: it takes that table's publish gate EXCLUSIVELY (BlockPendingCommits) while we
		// hold it SHARED through publish (see table_gates above).
		publish_seq = next_publish_sequence++;
		RegisterPendingCommit(publish_seq);
	}

	// commit the UndoBuffer of the transaction
	if (!error.HasError()) {
		if (HasOtherTransactions(transaction)) {
			info.active_transactions = ActiveTransactionState::OTHER_TRANSACTIONS;
		} else {
			info.active_transactions = ActiveTransactionState::NO_OTHER_TRANSACTIONS;
		}
		if (defer_publish) {
			// write the WAL flush marker - the commit is published after the group fsync below. Conflicts were
			// already validated in WriteToWAL (before any entries were written). We registered as pending BEFORE
			// that validation: until we publish, no catalog version can be attached to any table (see
			// BlockPendingCommits), so the validation stays authoritative. flush_marker_offset is the WAL offset we
			// must SyncUpTo() to make this commit durable.
			error = transaction.CommitToWAL(db, info, std::move(commit_state), flush_marker_offset);
			if (error.HasError()) {
				FinishPendingCommit(publish_seq);
			}
		} else {
			// synchronous path: publishes under the transaction lock, so the commit id can be assigned now
			info.commit_id = GetCommitTimestamp();
			error = transaction.Commit(db, info, std::move(commit_state));
		}
	}
	// the WAL entries and flush marker (if any) have been appended - other shared committers may now append theirs.
	// We keep the (shared) WAL lock until publish; only the append serialization is released here, before the fsync.
	if (append_guard.owns_lock()) {
		append_guard.unlock();
	}

	if (error.HasError()) {
		DUCKDB_LOG(context, TransactionLogType, db, "Rollback (after failed commit)", info.commit_id);
		defer_publish = false;

		// COMMIT not successful: ROLLBACK.
		checkpoint_decision = CheckpointDecision(error.Message());
		transaction.commit_id = 0;

		auto rollback_error = transaction.Rollback();
		if (rollback_error.HasError()) {
			throw FatalException(
			    "Failed to rollback transaction. Cannot continue operation.\nOriginal Error: %s\nRollback Error: %s",
			    error.Message(), rollback_error.Message());
		}
	} else if (!defer_publish) {
		DUCKDB_LOG(context, TransactionLogType, db, "Commit", info.commit_id);
		last_commit = info.commit_id;

		// check if catalog changes were made
		if (transaction.catalog_version >= TRANSACTION_ID_START) {
			transaction.catalog_version = ++last_committed_version;
		}
	}
	// (deferred commits remain registered as pending: the commit is complete in the WAL but not yet visible)
	OnCommitCheckpointDecision(checkpoint_decision, transaction);

	if (!checkpoint_decision.can_checkpoint && lock) {
		// we won't checkpoint after all due to an error during commit: unlock the checkpoint lock again
		skip_wal_write_due_to_checkpoint = false;
		lock.reset();
	}

	if (defer_publish) {
		// deferred (group) commit: capture the WAL; the offset we must sync to is the one our flush marker returned
		// from CommitToWAL (flush_marker_offset), threaded through explicitly rather than re-read from the WAL - so
		// the durability target is exactly our marker and does not depend on no other commit having appended since.
		// We hold the WAL lock SHARED here, which prevents a concurrent checkpoint (EXCLUSIVE) from swapping the WAL
		// out from under us, so a plain pointer to the current WAL is safe across the fsync below.
		D_ASSERT(held_wal_lock);
		auto sync_wal = db.GetStorageManager().GetWAL();
		idx_t sync_target = flush_marker_offset;

		// release the transaction lock so other transactions can start/commit while we wait for the fsync, and their
		// flush markers are covered by the same fsync (or a later one). We KEEP the WAL lock held (in SHARED mode) all
		// the way through publish: it does not block other (shared) committers, but it does make a checkpoint/catalog
		// change (EXCLUSIVE) wait for us, so it cannot swap the WAL or attach a catalog version while our commit is
		// still being made durable and visible. Transactions that start in this window do not see our commit yet: it
		// is published below, after the fsync.
		t_lock.unlock();

		try {
			if (sync_wal) {
				// leader_pushes_batch: the flush markers were only appended to the in-memory WAL buffer
				// (FlushCommitMarker with push_to_os=false), so the sync leader pushes the whole batch to the OS in
				// one write() before its fsync - batching the write too, not just the fsync. The push runs under the
				// WAL flush_lock (not the WAL lock), so it cannot deadlock against a concurrent checkpoint / catalog
				// commit / synchronous commit that holds the WAL lock and waits.
				sync_wal->SyncUpTo(sync_target, true, /*leader_pushes_batch=*/true);
			}
		} catch (std::exception &ex) {
			// the WAL cannot be guaranteed to be durable, and the flush marker can no longer be truncated
			// (other transactions may have appended in the meantime): this is fatal
			FinishPendingCommit(publish_seq);
			ErrorData sync_error(ex);
			throw FatalException("Failed to sync WAL during commit. Cannot continue operation.\nError: %s",
			                     sync_error.Message());
		}

		// publish in commit order - this keeps recently_committed_transactions ordered on commit_id and ensures
		// that the visible state always corresponds to a prefix of the WAL (matching replay after a crash)
		WaitForPublishTurn(publish_seq);

		t_lock.lock();
		// now that it is our turn and we hold the transaction lock, assign the commit timestamp and publish: a
		// transaction that starts after this point observes this commit, one that started before it does not
		info.commit_id = GetCommitTimestamp();
		// other transactions may have started or committed while we were syncing - recompute the state
		if (HasOtherTransactions(transaction)) {
			info.active_transactions = ActiveTransactionState::OTHER_TRANSACTIONS;
		} else {
			info.active_transactions = ActiveTransactionState::NO_OTHER_TRANSACTIONS;
		}
		auto publish_error = transaction.PublishCommit(db, info);
		if (publish_error.HasError()) {
			// the commit is durable in the WAL and can no longer be rolled back: this is fatal
			// (after a restart, WAL replay applies the transaction, which is the correct committed state)
			t_lock.unlock();
			FinishPendingCommit(publish_seq);
			throw FatalException("Failed to publish commit after WAL write. Cannot continue operation.\nError: %s",
			                     publish_error.Message());
		}
		DUCKDB_LOG(context, TransactionLogType, db, "Commit", info.commit_id);
		// take the maximum: between our commit id assignment and this point the transaction lock was released,
		// so a commit with a higher id (e.g. a read-only commit) may have already updated last_commit
		last_commit = MaxValue<transaction_t>(last_commit, info.commit_id);
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
	if (!skip_wal_write_due_to_checkpoint && held_wal_lock) {
		held_wal_lock.reset();
	}
	// publish is complete: release the per-table gates so DDL on those tables can proceed
	table_gates.clear();
	modified_table_infos.clear();
	if (defer_publish) {
		// the commit is now published - wake up any waiters (later commits and checkpoints)
		FinishPendingCommit(publish_seq);
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
		options.wal_lock = held_wal_lock.get();
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
