
#include "common/exception.hpp"
#include "common/helper.hpp"

#include "transaction/transaction_manager.hpp"

using namespace duckdb;
using namespace std;

namespace duckdb {
transaction_t TRANSACTION_ID_START = 4294967296ULL; // 2^32
transaction_t MAXIMUM_QUERY_ID =
    std::numeric_limits<transaction_t>::max(); // 2^64
} // namespace duckdb

TransactionManager::TransactionManager() {
	// start timestamp starts at zero
	current_start_timestamp = 0;
	// transaction ID starts very high:
	// it should be much higher than the current start timestamp
	// if transaction_id < start_timestamp for any set of active transactions
	// uncommited data could be read by
	current_transaction_id = TRANSACTION_ID_START;
	// the current active query id
	current_query_number = 1;
}

Transaction *TransactionManager::StartTransaction() {
	// obtain the transaction lock during this function
	lock_guard<mutex> lock(transaction_lock);

	if (current_start_timestamp >= TRANSACTION_ID_START) {
		throw Exception("Cannot start more transactions, ran out of "
		                "transaction identifiers!");
	}

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
	// check for the lowest and highest start time in the list of transactions
	transaction_t lowest_start_time = TRANSACTION_ID_START;
	transaction_t lowest_active_query = MAXIMUM_QUERY_ID;
	for (size_t i = 0; i < active_transactions.size(); i++) {
		if (active_transactions[i].get() == transaction) {
			t_index = i;
		} else {
			lowest_start_time =
			    std::min(lowest_start_time, active_transactions[i]->start_time);
			lowest_active_query = std::min(
			    lowest_active_query, active_transactions[i]->active_query);
		}
	}
	assert(t_index != active_transactions.size());
	auto current_transaction = move(active_transactions[t_index]);
	// add the current transaction to the set of old transactions
	old_transactions.push_back(move(current_transaction));
	// remove the transaction from the set of currently active transactions
	active_transactions.erase(active_transactions.begin() + t_index);
	// now traverse the recently_committed transactions to see if we can remove
	// any
	size_t deleted_index = (size_t)-1;
	for (size_t i = 0; i < old_transactions.size(); i++) {
		assert(old_transactions[i]);
		if (old_transactions[i]->highest_active_query == 0) {
			if (old_transactions[i]->commit_id < lowest_start_time) {
				// changes made BEFORE this transaction are no longer relevant
				// we can cleanup the undo buffer

				// HOWEVER: any currently running QUERY can still be using
				// the version information after the cleanup!

				// if we remove the UndoBuffer immediately, we have a race
				// condition

				// we can only safely do the actual memory cleanup when all the
				// currently active queries have finished running! (actually,
				// when all the currently active scans have finished running...)
				old_transactions[i]->Cleanup();
				// store the current highest active query
				old_transactions[i]->highest_active_query =
				    current_query_number;
			}
		}
		if (old_transactions[i]->highest_active_query > 0 &&
		    (active_transactions.size() == 0 ||
		     old_transactions[i]->highest_active_query < lowest_active_query)) {
			// no transaction could possibly be using this transactions' data
			// anymore we can safely garbage collect it
			old_transactions.erase(old_transactions.begin() + i);
			i--;
		}
	}
}