//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/meta_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {
class AttachedDatabase;
class ClientContext;
class DatabaseInstance;
class SecretManager;
class SecretStorage;
struct DatabaseModificationType;
class Transaction;

enum class TransactionState { UNCOMMITTED, COMMITTED, ROLLED_BACK };

struct TransactionReference {
	explicit TransactionReference(Transaction &transaction_p)
	    : state(TransactionState::UNCOMMITTED), transaction(transaction_p) {
	}

	TransactionState state;
	Transaction &transaction;
};

//! The MetaTransaction manages multiple transactions for different attached databases
class MetaTransaction {
public:
	DUCKDB_API MetaTransaction(DatabaseInstance &db, shared_ptr<AttachedDatabase> temporary_objects,
	                           timestamp_t start_timestamp, transaction_t global_transaction_id);
	DUCKDB_API ~MetaTransaction();

	//! The timestamp when the transaction started
	timestamp_t start_timestamp;
	//! The global identifier of the transaction
	transaction_t global_transaction_id;
	//! The validity checker of the transaction
	ValidChecker transaction_validity;

public:
	DUCKDB_API static MetaTransaction &Get(ClientContext &context);
	//! The temporary objects (temp catalog) the transaction operates on: those of the connection that created it
	AttachedDatabase &GetTemporaryObjects() const {
		return *temporary_objects;
	}
	const shared_ptr<AttachedDatabase> &GetTemporaryObjectsPtr() const {
		return temporary_objects;
	}

	//! Makes the transaction joinable by other connections, returns its identifier
	string ShareTransaction();
	//! The identifier under which the transaction was shared (empty if not shared)
	string GetShareId();
	//! Whether the transaction was shared with other connections
	bool IsShared() {
		return !GetShareId().empty();
	}
	//! Adds a participant to the shared transaction. Returns false if the transaction already ended.
	//! An inheriting participant cannot keep the transaction alive on its own (see Leave).
	bool Join(bool inherited = false);
	struct LeaveResult {
		//! Whether the participant that left was the last one - it then has to commit or roll back
		bool last;
		//! Whether all participants that left voted to commit
		bool commit;
		//! Whether only inheriting participants remain: the transaction can no longer commit
		bool orphaned;
	};
	//! A participant leaves the transaction, voting for a commit or rollback. When the last participant that did not
	//! inherit the transaction leaves while inheriting ones remain, the transaction is rolled back.
	LeaveResult Leave(bool commit, bool inherited = false);
	//! Whether the transaction was committed or rolled back
	bool IsFinished() const {
		return finished;
	}
	void MarkFinished() {
		finished = true;
	}
	timestamp_t GetCurrentTransactionStartTimestamp() const {
		return start_timestamp;
	}

	//! The transaction for the database, started on behalf of the given context if it does not exist yet
	Transaction &GetTransaction(ClientContext &context, AttachedDatabase &db);
	optional_ptr<Transaction> TryGetTransaction(AttachedDatabase &db);
	void RemoveTransaction(AttachedDatabase &db);

	ErrorData Commit(ClientContext &context);
	void Rollback(ClientContext &context);
	// Finalize the transaction after a COMMIT of ROLLBACK.
	void Finalize(ClientContext &context);

	void SetReadOnly();
	bool IsReadOnly() const;
	void ModifyDatabase(ClientContext &context, AttachedDatabase &db, DatabaseModificationType modification);
	optional_ptr<AttachedDatabase> ModifiedDatabase() {
		lock_guard<mutex> guard(modification_lock);
		return modified_database;
	}
	const vector<reference<AttachedDatabase>> &OpenedTransactions() const {
		return all_transactions;
	}
	optional_ptr<AttachedDatabase> GetReferencedDatabase(const Identifier &name);
	shared_ptr<AttachedDatabase> GetReferencedDatabaseOwning(const Identifier &name);
	AttachedDatabase &UseDatabase(shared_ptr<AttachedDatabase> &database);
	void DetachDatabase(AttachedDatabase &database);

private:
	friend class SecretManager;

	//! Lock to prevent all_transactions and transactions from getting out of sync.
	mutex lock;
	//! The set of active transactions for each database.
	reference_map_t<AttachedDatabase, TransactionReference> transactions;
	//! The set of referenced databases in invocation order.
	vector<reference<AttachedDatabase>> all_transactions;
	//! Guards modified_database and the read/write state of the transactions (statements can modify concurrently)
	mutex modification_lock;
	//! The database we are modifying. We can only modify one database per meta transaction.
	optional_ptr<AttachedDatabase> modified_database;
	//! Whether the meta transaction is marked as read only.
	bool is_read_only;
	//! Lock for referenced_databases.
	mutex referenced_database_lock;
	//! The set of used (referenced) databases.
	reference_map_t<AttachedDatabase, shared_ptr<AttachedDatabase>> referenced_databases;
	//! Map of name -> database for databases that are in-use by this transaction.
	identifier_map_t<reference<AttachedDatabase>> used_databases;
	//! The temporary objects of the connection that created the transaction
	shared_ptr<AttachedDatabase> temporary_objects;
	//! Whether the transaction ended (committed or rolled back)
	atomic<bool> finished;
	//! Guards the participant state below
	mutex shared_lock;
	//! The number of connections participating in the transaction, starting with the one that created it
	idx_t participants = 1;
	//! The number of participants that inherited the transaction (included in participants)
	idx_t inherited_participants = 0;
	//! Whether a participant voted to roll back
	bool rollback_vote = false;
	//! The identifier the transaction was exported under
	string share_id;
	//! Secrets that only live for the duration of this transaction.
	unique_ptr<SecretStorage> transaction_secret_storage;
};

} // namespace duckdb
