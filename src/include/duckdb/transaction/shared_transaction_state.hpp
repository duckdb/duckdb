//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/shared_transaction_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class DuckTransaction;
class SharedTransactionLock;

//! State of a shared transaction: one connection owns the transaction, and every connection that joined it with
//! SET TRANSACTION SNAPSHOT reads it. The state outlives the transaction, because participants still hold it after
//! the owner has finished.
//!
//! It also decides the transaction's remaining lifetime. The owner cannot always destroy the transaction when it is
//! done, because a participant may still be reading: a streaming result holds the statement lock until it is
//! drained, and the owner cannot wait for that in a destructor. So the owner hands the transaction over instead,
//! and whoever is last out destroys it. Only that handoff defers; an explicit COMMIT or ROLLBACK runs inside a
//! statement holding the statement lock exclusively, so nobody else can be reading.
class SharedTransactionState {
public:
	SharedTransactionState(string token, shared_ptr<SharedTransactionLock> statement_lock,
	                       DuckTransaction &transaction);

public:
	//! The capability that SET TRANSACTION SNAPSHOT accepts.
	const string &GetToken() const {
		return token;
	}
	//! Serializes the statements of every connection taking part.
	const shared_ptr<SharedTransactionLock> &GetStatementLock() const {
		return statement_lock;
	}
	//! Whether the owner has committed or rolled back. Participants may then start no new statement, but one that
	//! is already running keeps going, because the transaction is still there for it to read.
	bool IsEnded() const {
		return ended;
	}
	void MarkEnded() {
		ended = true;
	}
	//! Whether the transaction itself is gone, so a borrowed reference to it must no longer be used.
	bool IsDestroyed() const {
		return destroyed;
	}
	//! Whether a statement on the owning connection failed and invalidated the transaction. It is now certain to
	//! roll back, and may hold the partially applied changes of the statement that failed, so nobody else may
	//! read it: participants can only detach, and no new connection may join.
	bool IsInvalidated() const {
		return invalidated;
	}
	void MarkInvalidated() {
		invalidated = true;
	}

	//! Hold the transaction alive across the gap between looking its token up and joining it. Returns false when
	//! the transaction is already gone.
	bool TryReserveJoin();
	//! Turn a reservation into full participation, without ever passing through zero holders.
	void CompleteJoin();
	//! Give up a reservation whose join did not happen, destroying the transaction if we were the last to hold it.
	void AbandonJoin();
	//! Drop this connection's participation, destroying the transaction if we were the last to hold it.
	void LeaveParticipation();
	//! Note that the owner is finishing but is still using the transaction, so nobody may destroy it yet. Returns
	//! false when nobody else holds it, in which case the owner destroys it itself.
	bool TryHandOffToParticipants();
	//! Complete a handoff once the owner is done with the transaction. If everyone else left in the meantime, this
	//! destroys the transaction here rather than leaving it orphaned.
	void CompleteHandOff();
	//! Note that the owner destroyed the transaction itself.
	void MarkTransactionDestroyed();

private:
	//! Take the transaction for destruction if this caller is the last one holding it. Caller holds `lock`.
	optional_ptr<DuckTransaction> ClaimForDestruction();
	//! Roll back and forget a claimed transaction. Caller must not hold `lock`.
	void Destroy(optional_ptr<DuckTransaction> claimed);

private:
	//! The capability that SET TRANSACTION SNAPSHOT accepts.
	const string token;
	//! Serializes the statements of every connection taking part.
	const shared_ptr<SharedTransactionLock> statement_lock;
	//! Set once the owner has committed or rolled back.
	atomic<bool> ended;
	//! Set once the transaction is gone. Reaching this always requires the statement lock exclusively, so it can
	//! never happen underneath a running participant.
	atomic<bool> destroyed;
	//! Set once a failed statement invalidated the transaction. The owner sets this while still holding the
	//! statement lock exclusively, so it is visible before any participant can start another statement.
	atomic<bool> invalidated;

	//! Guards everything below: the transaction's remaining lifetime is decided entirely under this lock.
	mutex lock;
	//! The transaction, while it is alive. Whoever destroys it takes it from here, so exactly one caller can.
	optional_ptr<DuckTransaction> transaction;
	//! Connections that have joined the transaction.
	idx_t participants;
	//! Reservations taken by a token lookup that has not joined yet.
	idx_t pending_joins;
	//! The owner is handing the transaction over but has yet to finish using it. Nobody may claim it meanwhile.
	bool hand_off_pending;
	//! The owner is done with the transaction and left destroying it to whoever is last out.
	bool owner_finished;
};

} // namespace duckdb
