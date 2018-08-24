
#include "common/helper.hpp"

#include "transaction/transaction_manager.hpp"

using namespace duckdb;
using namespace std;

namespace duckdb {
transaction_t TRANSACTION_ID_START = 4294967296ULL; // 2^32
}

TransactionManager::TransactionManager() {
	// start timestamp starts at zero
	current_start_timestamp = 0;
	// transaction ID starts very high:
	// it should be much higher than the current start timestamp
	// if transaction_id < start_timestamp for any set of active transactions
	// uncommited data could be read by
	current_transaction_id = TRANSACTION_ID_START;
}

Transaction *TransactionManager::StartTransaction() {
	// obtain the transaction lock during this function
	lock_guard<mutex> lock(transaction_lock);

	// obtain the start time and transaction ID of this transaction
	transaction_t start_time = current_start_timestamp++;
	transaction_t transaction_id = current_transaction_id++;

	// create the actual transaction
	auto transaction = make_unique<Transaction>(start_time, transaction_id);
	auto transaction_ptr = transaction.get();

	// store it in the set of active transactions
	active_transactions.push_back(move(transaction));
	return transaction_ptr;
}

void TransactionManager::CommitTransaction(Transaction *transaction) {
	// obtain the transaction lock during this function
	lock_guard<mutex> lock(transaction_lock);

	// obtain a commit id for the transaction
	transaction_t commit_id = current_start_timestamp++;

	// commit the UndoBuffer of the transaction
	transaction->Commit(commit_id);

	// remove the transaction id from the list of active transactions
	// potentially resulting in garbage collection
	RemoveTransaction(transaction);
}

void TransactionManager::RollbackTransaction(Transaction *transaction) {
	// obtain the transaction lock during this function
	lock_guard<mutex> lock(transaction_lock);

	// rollback the transaction
	transaction->Rollback();

	// remove the transaction id from the list of active transactions
	// potentially resulting in garbage collection
	RemoveTransaction(transaction);
}

void TransactionManager::RemoveTransaction(Transaction *transaction) {
	// remove the transaction from the list of active transactions
	size_t t_index = active_transactions.size();
	// check for the lowest start time in the list of transactions
	transaction_t lowest_start_time = TRANSACTION_ID_START;
	for (size_t i = 0; i < active_transactions.size(); i++) {
		if (active_transactions[i].get() == transaction) {
			t_index = i;
		} else if (active_transactions[i]->start_time < lowest_start_time) {
			lowest_start_time = active_transactions[i]->start_time;
		}
	}
	assert(t_index != active_transactions.size());
	auto current_transaction = move(active_transactions[t_index]);
	if (transaction->commit_id != 0) {
		// if the transaction was committed, add it to the recently commited
		// transactions
		recently_committed_transactions.push_back(move(current_transaction));
	}
	active_transactions.erase(active_transactions.begin() + t_index);
	// now traverse the recently_committed transactions to see if we can remove
	// any
	size_t deleted_index = (size_t)-1;
	for (size_t i = 0; i < recently_committed_transactions.size(); i++) {
		if (recently_committed_transactions[i] &&
		    recently_committed_transactions[i]->commit_id < lowest_start_time) {
			// changes made BEFORE this transaction are no longer relevant
			// we can garbage collect this transaction
			recently_committed_transactions[i] = nullptr;
			deleted_index = i;
		}
	}
	if (deleted_index != (size_t)-1) {
		// we could garbage collect transactions: remove them from the list
		recently_committed_transactions.erase(
		    recently_committed_transactions.begin(),
		    recently_committed_transactions.begin() + deleted_index);
	}
}