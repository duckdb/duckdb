#include "duckdb/transaction/duck_transaction_manager.hpp"

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

namespace duckdb {

DuckTransactionManager::DuckTransactionManager(AttachedDatabase &db) : TransactionManager(db) {
	// start timestamp starts at two
	current_start_timestamp = 2;
	// transaction ID starts very high:
	// it should be much higher than the current start timestamp
	// if transaction_id < start_timestamp for any set of active transactions
	// uncommited data could be read by
	current_transaction_id = TRANSACTION_ID_START;
	lowest_active_id = TRANSACTION_ID_START;
	lowest_active_start = MAX_TRANSACTION_ID;
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
	auto transaction = make_uniq<DuckTransaction>(*this, context, start_time, transaction_id);
	auto &transaction_ref = *transaction;

	// store it in the set of active transactions
	active_transactions.push_back(std::move(transaction));
	return transaction_ref;
}

DuckTransactionManager::CheckpointDecision::CheckpointDecision(string reason_p) : can_checkpoint(false), reason(std::move(reason_p)) {}

DuckTransactionManager::CheckpointDecision::CheckpointDecision(CheckpointType type) :
    can_checkpoint(true), type(type) {}

DuckTransactionManager::CheckpointDecision::~CheckpointDecision() {

}

DuckTransactionManager::CheckpointDecision DuckTransactionManager::CanCheckpoint(DuckTransaction &transaction, unique_ptr<StorageLockKey> &lock) {
	if (db.IsSystem()) {
		return CheckpointDecision("system transaction");
	}
	auto &storage_manager = db.GetStorageManager();
	if (storage_manager.InMemory()) {
		return CheckpointDecision("in memory db");
	}
	auto undo_properties = transaction.GetUndoProperties();
	if (!transaction.AutomaticCheckpoint(db, undo_properties)) {
		return CheckpointDecision("no reason to automatically checkpoint");
	}
	// try to lock the checkpoint lock
	lock = transaction.TryGetCheckpointLock();
	if (!lock) {
		return CheckpointDecision("Failed to obtain checkpoint lock - another thread is writing/checkpointing or another read transaction relies on data that is not yet committed");
	}
	auto checkpoint_type = CheckpointType::FULL_CHECKPOINT;
	if (undo_properties.has_updates || undo_properties.has_deletes) {
		// if we have made updates or deletes in this transaction we might need to change our strategy
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
			if (undo_properties.has_updates) {
				// this transaction has performed updates - we cannot checkpoint
				return CheckpointDecision("Transaction has performed updates and there are other transactions active\nActive transactions: " + other_transactions);
			} else {
				// this transaction has performed deletes - we cannot vacuum - initiate a concurrent checkpoint instead
				D_ASSERT(undo_properties.has_deletes);
				checkpoint_type = CheckpointType::CONCURRENT_CHECKPOINT;
			}
		}
	}
	return CheckpointDecision(checkpoint_type);
}

void DuckTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &storage_manager = db.GetStorageManager();
	if (storage_manager.InMemory()) {
		return;
	}

	auto &current = DuckTransaction::Get(context, db);
	if (current.ChangesMade()) {
		throw TransactionException("Cannot CHECKPOINT: the current transaction has transaction local changes");
	}
	// try to get the checkpoint lock
	auto lock = checkpoint_lock.TryGetExclusiveLock();
	if (!lock && !force) {
		throw TransactionException(
		    "Cannot CHECKPOINT: there are other write transactions active. Use FORCE CHECKPOINT to abort "
		    "the other transactions and force a checkpoint");
	}
	if (force) {
		// lock all the clients AND the connection manager now
		// this ensures no new queries can be started, and no new connections to the database can be made
		// to avoid deadlock we release the transaction lock while locking the clients
		auto &connection_manager = ConnectionManager::Get(context);
		vector<ClientLockWrapper> client_locks;
		connection_manager.LockClients(client_locks, context);

		for (idx_t i = 0; i < active_transactions.size(); i++) {
			auto &transaction = active_transactions[i];
			// rollback the transaction
			transaction->Rollback();
			auto transaction_context = transaction->context.lock();

			// remove the transaction id from the list of active transactions
			// potentially resulting in garbage collection
			RemoveTransaction(*transaction);
			if (transaction_context) {
				// invalidate the active transaction for this connection
				auto &meta_transaction = MetaTransaction::Get(*transaction_context);
				meta_transaction.RemoveTransaction(db);
				ValidChecker::Get(meta_transaction).Invalidate("Invalidated due to FORCE CHECKPOINT");
			}
			i--;
		}
		lock = checkpoint_lock.TryGetExclusiveLock();
		if (!lock) {
			throw TransactionException(
			    "Cannot FORCE CHECKPOINT: failed to grab checkpoint lock after aborting all other transactions");
		}
	}
	CheckpointOptions options;
	if (GetLastCommit() > LowestActiveStart()) {
		// we cannot do a full checkpoint if any transaction needs to read old data
		options.type = CheckpointType::CONCURRENT_CHECKPOINT;
	}
	storage_manager.CreateCheckpoint(options);
}

unique_ptr<StorageLockKey> DuckTransactionManager::SharedCheckpointLock() {
	return checkpoint_lock.GetSharedLock();
}

unique_ptr<StorageLockKey> DuckTransactionManager::TryUpgradeCheckpointLock(unique_ptr<StorageLockKey> &lock) {
	if (!lock) {
		throw InternalException("TryUpgradeCheckpointLock - but thread has no shared lock!?");
	}
	return checkpoint_lock.TryUpgradeCheckpointLock(*lock);
}

transaction_t DuckTransactionManager::GetCommitTimestamp() {
	auto commit_ts = current_start_timestamp++;
	last_commit = commit_ts;
	return commit_ts;
}

ErrorData DuckTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction_p) {
	auto &transaction = transaction_p.Cast<DuckTransaction>();
	unique_lock<mutex> tlock(transaction_lock);
	if (!db.IsSystem() && !db.IsTemporary()) {
		if (transaction.ChangesMade()) {
			if (transaction.IsReadOnly()) {
				throw InternalException("Attempting to commit a transaction that is read-only but has made changes - "
				                        "this should not be possible");
			}
		}
	}
	// obtain a commit id for the transaction
	transaction_t commit_id = GetCommitTimestamp();

	// check if we can checkpoint
	unique_ptr<StorageLockKey> lock;
	auto checkpoint_decision = CanCheckpoint(transaction, lock);
	// commit the UndoBuffer of the transaction
	auto error = transaction.Commit(db, commit_id, checkpoint_decision.can_checkpoint);
	if (error.HasError()) {
		// commit unsuccessful: rollback the transaction instead
		checkpoint_decision = CheckpointDecision(error.Message());
		transaction.commit_id = 0;
		transaction.Rollback();
	}
	OnCommitCheckpointDecision(checkpoint_decision, transaction);

	if (!checkpoint_decision.can_checkpoint && lock) {
		// we won't checkpoint after all: unlock the checkpoint lock again
		lock.reset();
	}

	// commit successful: remove the transaction id from the list of active transactions
	// potentially resulting in garbage collection
	RemoveTransaction(transaction);
	// now perform a checkpoint if (1) we are able to checkpoint, and (2) the WAL has reached sufficient size to
	// checkpoint
	if (checkpoint_decision.can_checkpoint) {
		D_ASSERT(lock);
		// we can unlock the transaction lock while checkpointing
		tlock.unlock();
		// checkpoint the database to disk
		auto &storage_manager = db.GetStorageManager();
		CheckpointOptions options;
		options.action = CheckpointAction::FORCE_CHECKPOINT;
		options.type = checkpoint_decision.type;
		storage_manager.CreateCheckpoint(options);
	}
	return error;
}

void DuckTransactionManager::RollbackTransaction(Transaction &transaction_p) {
	auto &transaction = transaction_p.Cast<DuckTransaction>();
	// obtain the transaction lock during this function
	lock_guard<mutex> lock(transaction_lock);

	// rollback the transaction
	transaction.Rollback();

	// remove the transaction id from the list of active transactions
	// potentially resulting in garbage collection
	RemoveTransaction(transaction);
}

void DuckTransactionManager::RemoveTransaction(DuckTransaction &transaction) noexcept {
	bool changes_made = transaction.ChangesMade();
	// remove the transaction from the list of active transactions
	idx_t t_index = active_transactions.size();
	// check for the lowest and highest start time in the list of transactions
	transaction_t lowest_start_time = TRANSACTION_ID_START;
	transaction_t lowest_transaction_id = MAX_TRANSACTION_ID;
	transaction_t lowest_active_query = MAXIMUM_QUERY_ID;
	for (idx_t i = 0; i < active_transactions.size(); i++) {
		if (active_transactions[i].get() == &transaction) {
			t_index = i;
		} else {
			transaction_t active_query = active_transactions[i]->active_query;
			lowest_start_time = MinValue(lowest_start_time, active_transactions[i]->start_time);
			lowest_active_query = MinValue(lowest_active_query, active_query);
			lowest_transaction_id = MinValue(lowest_transaction_id, active_transactions[i]->transaction_id);
		}
	}
	lowest_active_start = lowest_start_time;
	lowest_active_id = lowest_transaction_id;

	transaction_t lowest_stored_query = lowest_start_time;
	D_ASSERT(t_index != active_transactions.size());
	auto current_transaction = std::move(active_transactions[t_index]);
	auto current_query = DatabaseManager::Get(db).ActiveQueryNumber();
	if (changes_made) {
		// if the transaction made any changes we need to keep it around
		if (transaction.commit_id != 0) {
			// the transaction was committed, add it to the list of recently
			// committed transactions
			recently_committed_transactions.push_back(std::move(current_transaction));
		} else {
			// the transaction was aborted, but we might still need its information
			// add it to the set of transactions awaiting GC
			current_transaction->highest_active_query = current_query;
			old_transactions.push_back(std::move(current_transaction));
		}
	}
	// remove the transaction from the set of currently active transactions
	active_transactions.unsafe_erase_at(t_index);
	// traverse the recently_committed transactions to see if we can remove any
	idx_t i = 0;
	for (; i < recently_committed_transactions.size(); i++) {
		D_ASSERT(recently_committed_transactions[i]);
		lowest_stored_query = MinValue(recently_committed_transactions[i]->start_time, lowest_stored_query);
		if (recently_committed_transactions[i]->commit_id < lowest_start_time) {
			// changes made BEFORE this transaction are no longer relevant
			// we can cleanup the undo buffer

			// HOWEVER: any currently running QUERY can still be using
			// the version information after the cleanup!

			// if we remove the UndoBuffer immediately, we have a race
			// condition

			// we can only safely do the actual memory cleanup when all the
			// currently active queries have finished running! (actually,
			// when all the currently active scans have finished running...)
			recently_committed_transactions[i]->Cleanup();
			// store the current highest active query
			recently_committed_transactions[i]->highest_active_query = current_query;
			// move it to the list of transactions awaiting GC
			old_transactions.push_back(std::move(recently_committed_transactions[i]));
		} else {
			// recently_committed_transactions is ordered on commit_id
			// implicitly thus if the current one is bigger than
			// lowest_start_time any subsequent ones are also bigger
			break;
		}
	}
	if (i > 0) {
		// we garbage collected transactions: remove them from the list
		recently_committed_transactions.erase(recently_committed_transactions.begin(),
		                                      recently_committed_transactions.begin() + static_cast<int64_t>(i));
	}
	// check if we can free the memory of any old transactions
	i = active_transactions.empty() ? old_transactions.size() : 0;
	for (; i < old_transactions.size(); i++) {
		D_ASSERT(old_transactions[i]);
		D_ASSERT(old_transactions[i]->highest_active_query > 0);
		if (old_transactions[i]->highest_active_query >= lowest_active_query) {
			// there is still a query running that could be using
			// this transactions' data
			break;
		}
	}
	if (i > 0) {
		// we garbage collected transactions: remove them from the list
		old_transactions.erase(old_transactions.begin(), old_transactions.begin() + static_cast<int64_t>(i));
	}
}

} // namespace duckdb
