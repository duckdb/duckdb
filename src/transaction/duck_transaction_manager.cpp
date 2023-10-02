#include "duckdb/transaction/duck_transaction_manager.hpp"

#include "duckdb/catalog/catalog_set.hpp"
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

namespace duckdb {

struct CheckpointLock {
	explicit CheckpointLock(DuckTransactionManager &manager) : manager(manager), is_locked(false) {
	}
	~CheckpointLock() {
		Unlock();
	}

	DuckTransactionManager &manager;
	bool is_locked;

	void Lock() {
		D_ASSERT(!manager.thread_is_checkpointing);
		manager.thread_is_checkpointing = true;
		is_locked = true;
	}
	void Unlock() {
		if (!is_locked) {
			return;
		}
		D_ASSERT(manager.thread_is_checkpointing);
		manager.thread_is_checkpointing = false;
		is_locked = false;
	}
};

DuckTransactionManager::DuckTransactionManager(AttachedDatabase &db)
    : TransactionManager(db), thread_is_checkpointing(false) {
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

Transaction *DuckTransactionManager::StartTransaction(ClientContext &context) {
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
	auto transaction_ptr = transaction.get();

	// store it in the set of active transactions
	active_transactions.push_back(std::move(transaction));
	return transaction_ptr;
}

struct ClientLockWrapper {
	ClientLockWrapper(mutex &client_lock, shared_ptr<ClientContext> connection)
	    : connection(std::move(connection)), connection_lock(make_uniq<lock_guard<mutex>>(client_lock)) {
	}

	shared_ptr<ClientContext> connection;
	unique_ptr<lock_guard<mutex>> connection_lock;
};

void DuckTransactionManager::LockClients(vector<ClientLockWrapper> &client_locks, ClientContext &context) {
	auto &connection_manager = ConnectionManager::Get(context);
	client_locks.emplace_back(connection_manager.connections_lock, nullptr);
	auto connection_list = connection_manager.GetConnectionList();
	for (auto &con : connection_list) {
		if (con.get() == &context) {
			continue;
		}
		auto &context_lock = con->context_lock;
		client_locks.emplace_back(context_lock, std::move(con));
	}
}

void DuckTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &storage_manager = db.GetStorageManager();
	if (storage_manager.InMemory()) {
		return;
	}

	// first check if no other thread is checkpointing right now
	auto lock = unique_lock<mutex>(transaction_lock);
	if (thread_is_checkpointing) {
		throw TransactionException("Cannot CHECKPOINT: another thread is checkpointing right now");
	}
	CheckpointLock checkpoint_lock(*this);
	checkpoint_lock.Lock();
	lock.unlock();

	// lock all the clients AND the connection manager now
	// this ensures no new queries can be started, and no new connections to the database can be made
	// to avoid deadlock we release the transaction lock while locking the clients
	vector<ClientLockWrapper> client_locks;
	LockClients(client_locks, context);

	auto current = &DuckTransaction::Get(context, db);
	lock.lock();
	if (current->ChangesMade()) {
		throw TransactionException("Cannot CHECKPOINT: the current transaction has transaction local changes");
	}
	if (!force) {
		if (!CanCheckpoint(current)) {
			throw TransactionException("Cannot CHECKPOINT: there are other transactions. Use FORCE CHECKPOINT to abort "
			                           "the other transactions and force a checkpoint");
		}
	} else {
		if (!CanCheckpoint(current)) {
			for (size_t i = 0; i < active_transactions.size(); i++) {
				auto &transaction = active_transactions[i];
				// rollback the transaction
				transaction->Rollback();
				auto transaction_context = transaction->context.lock();

				// remove the transaction id from the list of active transactions
				// potentially resulting in garbage collection
				RemoveTransaction(*transaction);
				if (transaction_context) {
					transaction_context->transaction.ClearTransaction();
				}
				i--;
			}
			D_ASSERT(CanCheckpoint(nullptr));
		}
	}
	storage_manager.CreateCheckpoint();
}

bool DuckTransactionManager::CanCheckpoint(optional_ptr<DuckTransaction> current) {
	if (db.IsSystem()) {
		return false;
	}
	auto &storage_manager = db.GetStorageManager();
	if (storage_manager.InMemory()) {
		return false;
	}
	if (!recently_committed_transactions.empty() || !old_transactions.empty()) {
		return false;
	}
	for (auto &transaction : active_transactions) {
		if (transaction.get() != current.get()) {
			return false;
		}
	}
	return true;
}

string DuckTransactionManager::CommitTransaction(ClientContext &context, Transaction *transaction_p) {
	auto &transaction = transaction_p->Cast<DuckTransaction>();
	vector<ClientLockWrapper> client_locks;
	auto lock = make_uniq<lock_guard<mutex>>(transaction_lock);
	CheckpointLock checkpoint_lock(*this);
	// check if we can checkpoint
	bool checkpoint = thread_is_checkpointing ? false : CanCheckpoint(&transaction);
	if (checkpoint) {
		if (transaction.AutomaticCheckpoint(db)) {
			checkpoint_lock.Lock();
			// we might be able to checkpoint: lock all clients
			// to avoid deadlock we release the transaction lock while locking the clients
			lock.reset();

			LockClients(client_locks, context);

			lock = make_uniq<lock_guard<mutex>>(transaction_lock);
			checkpoint = CanCheckpoint(&transaction);
			if (!checkpoint) {
				checkpoint_lock.Unlock();
				client_locks.clear();
			}
		} else {
			checkpoint = false;
		}
	}
	// obtain a commit id for the transaction
	transaction_t commit_id = current_start_timestamp++;
	// commit the UndoBuffer of the transaction
	string error = transaction.Commit(db, commit_id, checkpoint);
	if (!error.empty()) {
		// commit unsuccessful: rollback the transaction instead
		checkpoint = false;
		transaction.commit_id = 0;
		transaction.Rollback();
	}
	if (!checkpoint) {
		// we won't checkpoint after all: unlock the clients again
		checkpoint_lock.Unlock();
		client_locks.clear();
	}

	// commit successful: remove the transaction id from the list of active transactions
	// potentially resulting in garbage collection
	RemoveTransaction(transaction);
	// now perform a checkpoint if (1) we are able to checkpoint, and (2) the WAL has reached sufficient size to
	// checkpoint
	if (checkpoint) {
		// checkpoint the database to disk
		auto &storage_manager = db.GetStorageManager();
		storage_manager.CreateCheckpoint(false, true);
	}
	return error;
}

void DuckTransactionManager::RollbackTransaction(Transaction *transaction_p) {
	auto &transaction = transaction_p->Cast<DuckTransaction>();
	// obtain the transaction lock during this function
	lock_guard<mutex> lock(transaction_lock);

	// rollback the transaction
	transaction.Rollback();

	// remove the transaction id from the list of active transactions
	// potentially resulting in garbage collection
	RemoveTransaction(transaction);
}

void DuckTransactionManager::RemoveTransaction(DuckTransaction &transaction) noexcept {
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
	// remove the transaction from the set of currently active transactions
	active_transactions.erase(active_transactions.begin() + t_index);
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
		                                      recently_committed_transactions.begin() + i);
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
		old_transactions.erase(old_transactions.begin(), old_transactions.begin() + i);
	}
}

} // namespace duckdb
