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
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"

#include <condition_variable>

namespace duckdb {
class AttachedDatabase;
class ClientContext;
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
	DUCKDB_API MetaTransaction(ClientContext &context, timestamp_t start_timestamp,
	                           transaction_t global_transaction_id);

	ClientContext &context;
	//! The timestamp when the transaction started
	timestamp_t start_timestamp;
	//! The global identifier of the transaction
	transaction_t global_transaction_id;
	//! The validity checker of the transaction
	ValidChecker transaction_validity;
	//! The active query number
	atomic<transaction_t> active_query;

public:
	DUCKDB_API static MetaTransaction &Get(ClientContext &context);
	timestamp_t GetCurrentTransactionStartTimestamp() const {
		return start_timestamp;
	}

	Transaction &GetTransaction(AttachedDatabase &db);
	optional_ptr<Transaction> TryGetTransaction(AttachedDatabase &db);
	void RemoveTransaction(AttachedDatabase &db);

	ErrorData Commit();
	void Rollback();
	// Finalize the transaction after a COMMIT of ROLLBACK.
	void Finalize();

	idx_t GetActiveQuery();
	void SetActiveQuery(transaction_t query_number);

	void SetReadOnly();
	bool IsReadOnly() const;

	//! ===== Shared transaction support =====
	//! Mark this transaction as shared (callable only by the owning context).
	void MarkShared();
	bool IsShared() const {
		return is_shared.load();
	}
	//! Whether the given context is the owner of this transaction.
	bool IsOwner(ClientContext &candidate) const {
		return &candidate == owner_context;
	}
	//! Increase the participant count (called when a participant attaches via SET TRANSACTION SNAPSHOT).
	void AttachParticipant();
	//! Decrease the participant count (called when a participant detaches). Returns the new count.
	idx_t DetachParticipant();
	//! Block until participant_count drops to 1 (owner-only). Polls for interruption on
	//! the supplied ClientContext so the caller can Ctrl-C a stuck COMMIT. Returns true
	//! when participants have detached, false if interrupted.
	bool WaitForParticipantsToDetach(ClientContext &waiter);
	//! Force all participants to detach on next statement (owner rollback path).
	void ForceDetach();
	bool IsForceDetached() const {
		return force_detached.load();
	}
	//! Mark the transaction as committing. While set, the snapshot registry rejects
	//! new participant imports — without this, a late `SET TRANSACTION SNAPSHOT`
	//! during the owner's COMMIT wait could increment `participant_count` and
	//! re-block the owner indefinitely (livelock under adversarial timing).
	void MarkCommitting() {
		committing.store(true);
	}
	bool IsCommitting() const {
		return committing.load();
	}
	idx_t ParticipantCount() const {
		return participant_count.load();
	}
	//! Acquire the per-meta statement lock. Held for the duration of a query that
	//! runs against the shared transaction so that owner + participant statements
	//! serialize, preserving the storage layer's single-writer-per-Transaction
	//! invariant. Returns a `unique_lock` so the caller controls scope (matches
	//! the `PartialBlockManager::GetLock()` idiom).
	unique_lock<mutex> LockSharedStatement() {
		return unique_lock<mutex>(shared_statement_lock);
	}
	void ModifyDatabase(AttachedDatabase &db, DatabaseModificationType modification);
	optional_ptr<AttachedDatabase> ModifiedDatabase() {
		return modified_database;
	}
	const vector<reference<AttachedDatabase>> &OpenedTransactions() const {
		return all_transactions;
	}
	optional_ptr<AttachedDatabase> GetReferencedDatabase(const string &name);
	shared_ptr<AttachedDatabase> GetReferencedDatabaseOwning(const string &name);
	AttachedDatabase &UseDatabase(shared_ptr<AttachedDatabase> &database);
	void DetachDatabase(AttachedDatabase &database);

private:
	//! Lock to prevent all_transactions and transactions from getting out of sync.
	mutex lock;
	//! The set of active transactions for each database.
	reference_map_t<AttachedDatabase, TransactionReference> transactions;
	//! The set of referenced databases in invocation order.
	vector<reference<AttachedDatabase>> all_transactions;
	//! The database we are modifying. We can only modify one database per meta transaction.
	optional_ptr<AttachedDatabase> modified_database;
	//! Whether the meta transaction is marked as read only.
	bool is_read_only;
	//! Lock for referenced_databases.
	mutex referenced_database_lock;
	//! The set of used (referenced) databases.
	reference_map_t<AttachedDatabase, shared_ptr<AttachedDatabase>> referenced_databases;
	//! Map of name -> database for databases that are in-use by this transaction.
	case_insensitive_map_t<reference<AttachedDatabase>> used_databases;

	//! ===== Shared transaction state =====
	//! The originating ClientContext (owner). Raw pointer for identity comparison only.
	ClientContext *owner_context;
	//! Whether this transaction is currently shared with one or more participants.
	atomic<bool> is_shared;
	//! Number of contexts attached to this transaction (1 = owner-only).
	atomic<idx_t> participant_count;
	//! Set by owner Rollback so participants' next statement errors out.
	atomic<bool> force_detached;
	//! Set by owner Commit so the registry refuses new imports during the
	//! wait-for-detach phase. See MarkCommitting() for rationale.
	atomic<bool> committing {false};
	//! Mutex serializing concurrent statements from owner + participants. Held by the
	//! `ActiveQueryContext` of whatever query is executing against this shared meta.
	mutex shared_statement_lock;
	//! Condition variable + mutex used by owner Commit / Rollback to wait on
	//! participant detach. The cv is signalled by `DetachParticipant` and
	//! `ForceDetach` while holding `detach_mutex`.
	mutex detach_mutex;
	std::condition_variable detach_cv;
};

} // namespace duckdb
