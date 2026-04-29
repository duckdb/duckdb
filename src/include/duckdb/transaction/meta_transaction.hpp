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

namespace duckdb {
class AttachedDatabase;
class ClientContext;
struct DatabaseModificationType;
class Transaction;
class DuckTransaction;

//! Result of MetaTransaction::TryClaimParticipant. The TryAddParticipant() bump on the
//! returned DuckTransaction has already been performed; the caller is responsible for either
//! a successful ImportTransaction (consumes the bump) or a CancelParticipation (releases it).
//! Both fields are non-null on a successful claim.
struct ClaimParticipantResult {
	AttachedDatabase *database = nullptr;
	DuckTransaction *transaction = nullptr;
};

enum class TransactionState { UNCOMMITTED, COMMITTED, ROLLED_BACK };

struct TransactionReference {
	explicit TransactionReference(Transaction &transaction_p, bool is_joined_p = false)
	    : state(TransactionState::UNCOMMITTED), transaction(transaction_p), is_joined(is_joined_p) {
	}

	TransactionState state;
	Transaction &transaction;
	//! True if this transaction was joined via JOIN TRANSACTION (the underlying DuckTransaction
	//! is owned by another connection's MetaTransaction). Joined transactions detach via
	//! DuckTransaction::Finalize instead of going through TransactionManager::Commit /
	//! Rollback for this MetaTransaction.
	bool is_joined;

	//! Whether this reference's lifecycle is shared with other MetaTransactions and therefore
	//! requires the participant-count finalize path. True when:
	//!   - we joined it (foreign DuckTransaction we don't own), or
	//!   - we own it but other participants are currently attached (IsShared), or
	//!   - we own it but a previous detacher has already voted rollback (RollbackRequested),
	//!     in which case we still need the doom-aware finalize even if no participants remain.
	//! This is the canonical "do not call TransactionManager::Commit/Rollback directly" check.
	bool IsShared() const;
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
	//! Import an existing DuckTransaction belonging to another connection. The participant_count
	//! on the DuckTransaction must already have been bumped by the caller (via TryAddParticipant).
	void ImportTransaction(AttachedDatabase &db, Transaction &transaction);
	//! Atomically look up a DuckTransaction by database name in this MetaTransaction's open
	//! transactions and call TryAddParticipant() on it under the meta lock. Returns an empty
	//! result if no matching transaction exists, the match is not a DuckTransaction, or the
	//! transaction has already been finalized. The lookup-and-bump is atomic with respect to
	//! RemoveTransaction(), so the returned DuckTransaction reference cannot be destroyed by
	//! a concurrent DETACH between the lookup and the bump. The caller is responsible for
	//! either ImportTransaction (consumes the bump) or CancelParticipation on the returned
	//! DuckTransaction. Naming: this is called from the joiner's perspective — we are
	//! claiming a participant slot in the *owner's* transaction.
	ClaimParticipantResult TryClaimParticipant(const string &db_name);
	//! Returns the previously-pinned shared-transaction id for this MetaTransaction (set via
	//! SetSharedTransactionId on the first duckdb_share_transaction() call), or an empty string
	//! if none has been pinned yet.
	const string &GetSharedTransactionId() const {
		return shared_transaction_id;
	}
	//! Pin a shared-transaction id on first call. Subsequent duckdb_share_transaction() calls in
	//! the same MetaTransaction return the cached value rather than recomputing.
	void SetSharedTransactionId(const string &id) {
		shared_transaction_id = id;
	}
	//! Whether this MetaTransaction is part of a shared session (in either role): it has
	//! imported a transaction from another connection, or another connection has imported a
	//! transaction it owns. Used by ATTACH/DETACH guards.
	bool IsParticipatingInSharedTransaction();

	ErrorData Commit();
	void Rollback();
	// Finalize the transaction after a COMMIT of ROLLBACK.
	void Finalize();

	idx_t GetActiveQuery();
	void SetActiveQuery(transaction_t query_number);

	void SetReadOnly();
	bool IsReadOnly() const;
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
	//! Cached shared-transaction id (pinned on the first duckdb_share_transaction() call within
	//! this MetaTransaction). Empty until pinned. Stable for the lifetime of this transaction.
	string shared_transaction_id;
};

} // namespace duckdb
