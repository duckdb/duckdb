#include "duckdb/transaction/meta_transaction.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/secret/secret_storage.hpp"

namespace duckdb {

MetaTransaction::MetaTransaction(ClientContext &context_p, timestamp_t start_timestamp_p,
                                 transaction_t transaction_id_p)
    : context(context_p), start_timestamp(start_timestamp_p), global_transaction_id(transaction_id_p),
      transaction_validity(*context_p.db), active_query(MAXIMUM_QUERY_ID), modified_database(nullptr),
      is_read_only(false) {
}

MetaTransaction::~MetaTransaction() = default;

MetaTransaction &MetaTransaction::Get(ClientContext &context) {
	return context.transaction.ActiveTransaction();
}

ValidChecker &ValidChecker::Get(MetaTransaction &transaction) {
	return transaction.transaction_validity;
}

Transaction &Transaction::Get(ClientContext &context, AttachedDatabase &db) {
	auto &meta_transaction = MetaTransaction::Get(context);
	return meta_transaction.GetTransaction(db);
}

optional_ptr<Transaction> Transaction::TryGet(ClientContext &context, AttachedDatabase &db) {
	auto &meta_transaction = MetaTransaction::Get(context);
	return meta_transaction.TryGetTransaction(db);
}

#ifdef DEBUG
static void VerifyAllTransactionsUnique(AttachedDatabase &db, vector<reference<AttachedDatabase>> &all_transactions) {
	for (auto &tx : all_transactions) {
		if (RefersToSameObject(db, tx.get())) {
			throw InternalException("Database is already present in all_transactions");
		}
	}
}
#endif

optional_ptr<Transaction> MetaTransaction::TryGetTransaction(AttachedDatabase &db) {
	lock_guard<mutex> guard(lock);
	auto entry = transactions.find(db);
	if (entry == transactions.end()) {
		return nullptr;
	}
	if (entry->second.borrowed) {
		// Only safe to hand out while this connection holds the statement lock: its owner takes that lock
		// exclusively to destroy the transaction, so it cannot go away underneath the caller.
		D_ASSERT(context.HasSharedTransactionGuard());
		if (shared.state->IsDestroyed()) {
			return nullptr;
		}
	}
	return &entry->second.transaction;
}

Transaction &MetaTransaction::GetTransaction(AttachedDatabase &db) {
	if (ValidChecker::IsInvalidated(db)) {
		throw IOException("%s", ValidChecker::InvalidatedMessage(db));
	}
	lock_guard<mutex> guard(lock);
	auto entry = transactions.find(db);
	if (entry == transactions.end()) {
		auto &new_transaction = db.GetTransactionManager().StartTransaction(context);
		new_transaction.active_query = active_query.load();
#ifdef DEBUG
		VerifyAllTransactionsUnique(db, all_transactions);
#endif
		// Rollback looks every entry of all_transactions up in transactions, so the two must not get out of sync:
		// reserve first, then insert, so that a failing allocation happens before either is modified and the
		// push_back that follows cannot allocate.
		all_transactions.reserve(all_transactions.size() + 1);
		transactions.insert(make_pair(reference<AttachedDatabase>(db), TransactionReference(new_transaction)));
		all_transactions.push_back(db);
		auto shared_db = db.shared_from_this();
		UseDatabase(shared_db);

		return new_transaction;
	} else {
		if (entry->second.borrowed) {
			// See TransactionReference::borrowed: the statement lock is what keeps this reference alive.
			D_ASSERT(context.HasSharedTransactionGuard());
			if (shared.state->IsDestroyed()) {
				throw TransactionException("Shared transaction has ended: the owning connection has committed or "
				                           "rolled back. COMMIT or ROLLBACK detaches from it");
			}
		}
		auto &transaction = entry->second.transaction;
		D_ASSERT(entry->second.borrowed ||
		         (transaction.IsDuckTransaction() && transaction.Cast<DuckTransaction>().IsShared()) ||
		         transaction.active_query == active_query);
		return transaction;
	}
}

void MetaTransaction::RemoveTransaction(AttachedDatabase &db) {
	auto entry = transactions.find(db);
	if (entry == transactions.end()) {
		throw InternalException("MetaTransaction::RemoveTransaction called but meta transaction did not have a "
		                        "transaction for this database");
	}
	transactions.erase(entry);
	for (idx_t i = 0; i < all_transactions.size(); i++) {
		auto &db_entry = all_transactions[i];
		if (RefersToSameObject(db_entry.get(), db)) {
			all_transactions.erase_at(i);
			break;
		}
	}
}

void MetaTransaction::SetReadOnly() {
	if (modified_database) {
		throw InternalException("Cannot set MetaTransaction to read only - modifications have already been made");
	}
	this->is_read_only = true;
}

bool MetaTransaction::IsReadOnly() const {
	return is_read_only;
}

Transaction &Transaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog.GetAttached());
}

void MetaTransaction::SetSharedTransaction(AttachedDatabase &db, shared_ptr<SharedTransactionState> state) {
	D_ASSERT(state);
	lock_guard<mutex> guard(lock);
	D_ASSERT(transactions.find(db) != transactions.end());
	D_ASSERT(!shared.database || RefersToSameObject(*shared.database, db));
	shared.database = &db;
	shared.state = std::move(state);
}

void MetaTransaction::ValidateSharableTransaction(AttachedDatabase &db) {
	lock_guard<mutex> guard(lock);
	if (shared.database && !RefersToSameObject(*shared.database, db)) {
		throw TransactionException("Cannot share transaction for database %s: this transaction already takes part "
		                           "in a shared transaction for database %s",
		                           db.GetName(), shared.database->GetName());
	}
}

void MetaTransaction::AdoptTransaction(AttachedDatabase &db, DuckTransaction &transaction) {
	D_ASSERT(transaction.IsShared());
	lock_guard<mutex> guard(lock);
	if (shared.database) {
		throw TransactionException("Cannot set the transaction snapshot for database %s: this connection already takes "
		                           "part in a shared transaction for database %s",
		                           db.GetName(), shared.database->GetName());
	}
	{
		lock_guard<mutex> referenced_guard(referenced_database_lock);
		auto used_entry = used_databases.find(db.GetName());
		if (used_entry != used_databases.end() && !RefersToSameObject(used_entry->second.get(), db)) {
			throw TransactionException("Cannot set the transaction snapshot for database %s: this name already refers "
			                           "to a different attached database in the current transaction",
			                           db.GetName());
		}
	}
	if (transactions.find(db) != transactions.end()) {
		throw TransactionException("SET TRANSACTION SNAPSHOT must be executed before any statement that uses "
		                           "database %s in the current transaction",
		                           db.GetName());
	}
#ifdef DEBUG
	VerifyAllTransactionsUnique(db, all_transactions);
#endif
	// Same ordering as GetTransaction: reserve, insert, then push_back, so a failing allocation changes nothing.
	all_transactions.reserve(all_transactions.size() + 1);
	transactions.insert({reference<AttachedDatabase>(db), TransactionReference(transaction, true)});
	all_transactions.push_back(db);
	auto shared_db = db.shared_from_this();
	UseDatabase(shared_db);
	shared.database = &db;
	shared.state = transaction.GetSharedState();
	shared.is_participant = true;
	// Turn the reservation JoinTransaction took into full participation without ever dropping to zero holders.
	shared.state->CompleteJoin();
}

void MetaTransaction::CompleteSharedHandOff() {
	if (!shared.state || shared.is_participant) {
		return;
	}
	// Everything we needed the transaction for is done, so the participants may now destroy it.
	shared.state->CompleteHandOff();
}

void MetaTransaction::MarkSharedTransactionDestroyed(AttachedDatabase &db) {
	// Only the owning connection reaches the manager for the shared transaction, and only under the statement lock.
	if (!shared.state || shared.is_participant || !shared.database) {
		return;
	}
	if (RefersToSameObject(*shared.database, db)) {
		shared.state->MarkTransactionDestroyed();
	}
}

void MetaTransaction::LeaveSharedTransaction() {
	if (!shared.state || !shared.is_participant) {
		return;
	}
	// If we are the last holder of a transaction whose owner is gone, this cleans it up on its behalf.
	shared.state->LeaveParticipation();
}

void MetaTransaction::EndSharedTransaction() {
	if (!shared.state || shared.is_participant) {
		return;
	}
	auto entry = transactions.find(*shared.database);
	D_ASSERT(entry != transactions.end());
	auto &transaction = entry->second.transaction.Cast<DuckTransaction>();
	transaction.GetTransactionManager().EndSharedTransaction(transaction);
}

ErrorData MetaTransaction::Commit() {
	ErrorData error;
	EndSharedTransaction();
	LeaveSharedTransaction();
#ifdef DEBUG
	reference_set_t<AttachedDatabase> committed_tx;
#endif
	// commit transactions in reverse order
	for (idx_t i = all_transactions.size(); i > 0; i--) {
		auto &db = all_transactions[i - 1].get();
		auto entry = transactions.find(db);
		if (entry == transactions.end()) {
			throw InternalException("Could not find transaction corresponding to database in MetaTransaction");
		}

#ifdef DEBUG
		auto already_committed = committed_tx.insert(db).second == false;
		if (already_committed) {
			throw InternalException("All databases inside all_transactions should be unique, invariant broken!");
		}
#endif

		auto &transaction_manager = db.GetTransactionManager();
		auto &transaction_ref = entry->second;
		if (transaction_ref.borrowed) {
			// We only read this transaction: its owner decides whether it commits.
			transaction_ref.state = TransactionState::COMMITTED;
			continue;
		}
		if (ValidChecker::IsInvalidated(db)) {
			error.Merge(ErrorData(IOException("%s", ValidChecker::InvalidatedMessage(db))));
			continue;
		}
		if (transaction_ref.state != TransactionState::UNCOMMITTED) {
			continue;
		}
		auto &transaction = transaction_ref.transaction;
		try {
			if (!error.HasError()) {
				// Commit the transaction.
				error = transaction_manager.CommitTransaction(context, transaction);
				transaction_ref.state = error.HasError() ? TransactionState::ROLLED_BACK : TransactionState::COMMITTED;
			} else {
				// Rollback due to previous error.
				transaction_manager.RollbackTransaction(transaction);
				transaction_ref.state = TransactionState::ROLLED_BACK;
			}
		} catch (std::exception &ex) {
			error.Merge(ErrorData(ex));
			transaction_ref.state = TransactionState::ROLLED_BACK;
		}
		MarkSharedTransactionDestroyed(db);
	}
	return error;
}

void MetaTransaction::Rollback(bool allow_hand_off) {
	// Rollback all transactions in reverse order.
	ErrorData error;
	EndSharedTransaction();
	LeaveSharedTransaction();
	for (idx_t i = all_transactions.size(); i > 0; i--) {
		auto &db = all_transactions[i - 1].get();
		auto &transaction_manager = db.GetTransactionManager();
		auto entry = transactions.find(db);
		D_ASSERT(entry != transactions.end());
		auto &transaction_ref = entry->second;
		if (transaction_ref.borrowed) {
			// We only read this transaction: its owner decides whether it rolls back.
			transaction_ref.state = TransactionState::ROLLED_BACK;
			continue;
		}
		if (allow_hand_off && shared.state && shared.database && RefersToSameObject(*shared.database, db)) {
			// Someone is still holding this. LockSharedTransactionForFinalize already recorded the hand-off, in
			// the same critical section that saw them, so whichever holder leaves last will roll it back.
			transaction_ref.state = TransactionState::ROLLED_BACK;
			continue;
		}
		if (ValidChecker::IsInvalidated(db)) {
			error.Merge(ErrorData(IOException("%s", ValidChecker::InvalidatedMessage(db))));
			continue;
		}
		if (transaction_ref.state != TransactionState::UNCOMMITTED) {
			continue;
		}
		try {
			auto &transaction = transaction_ref.transaction;
			transaction_manager.RollbackTransaction(transaction);
		} catch (std::exception &ex) {
			error.Merge(ErrorData(ex));
		}
		transaction_ref.state = TransactionState::ROLLED_BACK;
		MarkSharedTransactionDestroyed(db);
	}
	if (allow_hand_off) {
		// We are done with the transaction now; let whoever is still holding it clean it up.
		CompleteSharedHandOff();
	}
	if (error.HasError()) {
		error.Throw();
	}
}

void MetaTransaction::Finalize() {
	// Try to checkpoint any attached databases potentially still held by this transaction.
	for (auto &database : referenced_databases) {
		// If the use count is down to one, then we already detached the database.
		// That means new transactions can no longer obtain a shared pointer to it.
		AttachedDatabase::InvokeCloseIfLastReference(database.second, context);
	}
}

idx_t MetaTransaction::GetActiveQuery() {
	return active_query;
}

void MetaTransaction::SetActiveQuery(transaction_t query_number) {
	lock_guard<mutex> guard(lock);
	active_query = query_number;
	for (auto &entry : transactions) {
		if (entry.second.borrowed) {
			// Only the owning connection stamps its query number onto a transaction.
			continue;
		}
		entry.second.transaction.active_query = query_number;
	}
}

optional_ptr<AttachedDatabase> MetaTransaction::GetReferencedDatabase(const Identifier &name) {
	lock_guard<mutex> guard(referenced_database_lock);
	auto entry = used_databases.find(name);
	if (entry != used_databases.end()) {
		return entry->second.get();
	}
	return nullptr;
}

shared_ptr<AttachedDatabase> MetaTransaction::GetReferencedDatabaseOwning(const Identifier &name) {
	lock_guard<mutex> guard(referenced_database_lock);
	for (auto &entry : referenced_databases) {
		if (entry.first.get().name == name) {
			return entry.second;
		}
	}
	return nullptr;
}

void MetaTransaction::DetachDatabase(AttachedDatabase &database) {
	lock_guard<mutex> guard(referenced_database_lock);
	used_databases.erase(database.GetName());
}

AttachedDatabase &MetaTransaction::UseDatabase(shared_ptr<AttachedDatabase> &database) {
	auto &db_ref = *database;
	lock_guard<mutex> guard(referenced_database_lock);
	auto entry = referenced_databases.find(db_ref);
	if (entry == referenced_databases.end()) {
		auto used_entry = used_databases.emplace(db_ref.GetName(), db_ref);
		if (!used_entry.second) {
			// return used_entry.first->second.get();
			throw InternalException(
			    "Database name %s was already used by a different database for this meta transaction",
			    db_ref.GetName());
		}
		referenced_databases.emplace(reference<AttachedDatabase>(db_ref), database);
	}
	return db_ref;
}

void MetaTransaction::ModifyDatabase(AttachedDatabase &db, DatabaseModificationType modification) {
	if (IsReadOnly()) {
		throw TransactionException("Cannot write to database %s - transaction is launched in read-only mode",
		                           db.GetName());
	}
	if (shared.is_participant && RefersToSameObject(*shared.database, db)) {
		throw TransactionException("Cannot write to database %s - only the owning connection can modify a shared "
		                           "transaction",
		                           db.GetName());
	}
	auto &transaction = GetTransaction(db);
	if (transaction.IsReadOnly()) {
		transaction.SetReadWrite();
	}
	transaction.SetModifications(modification);
	if (db.IsSystem() || db.IsTemporary()) {
		// we can always modify the system and temp databases
		return;
	}
	if (!modified_database) {
		modified_database = &db;
		return;
	}
	if (&db != modified_database.get()) {
		throw TransactionException(
		    "Attempting to write to database %s in a transaction that has already modified database %s - a "
		    "single transaction can only write to a single attached database.",
		    db.GetName(), modified_database->GetName());
	}
}

} // namespace duckdb
