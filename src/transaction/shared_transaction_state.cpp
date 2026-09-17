#include "duckdb/transaction/shared_transaction_state.hpp"

#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

SharedTransactionState::SharedTransactionState(string token_p, shared_ptr<SharedTransactionLock> statement_lock_p,
                                               DuckTransaction &transaction_p)
    : token(std::move(token_p)), statement_lock(std::move(statement_lock_p)), ended(false), destroyed(false),
      invalidated(false), transaction(&transaction_p), participants(0), pending_joins(0), hand_off_pending(false),
      owner_finished(false) {
}

optional_ptr<DuckTransaction> SharedTransactionState::ClaimForDestruction() {
	if (!owner_finished || participants > 0 || pending_joins > 0 || !transaction) {
		return nullptr;
	}
	auto claimed = transaction;
	transaction = nullptr;
	return claimed;
}

void SharedTransactionState::Destroy(optional_ptr<DuckTransaction> claimed) {
	if (!claimed) {
		return;
	}
	claimed->GetTransactionManager().RollbackTransaction(*claimed);
	destroyed = true;
}

bool SharedTransactionState::TryReserveJoin() {
	lock_guard<mutex> guard(lock);
	if (!transaction) {
		return false;
	}
	pending_joins++;
	return true;
}

void SharedTransactionState::CompleteJoin() {
	lock_guard<mutex> guard(lock);
	D_ASSERT(pending_joins > 0);
	pending_joins--;
	participants++;
}

void SharedTransactionState::AbandonJoin() {
	optional_ptr<DuckTransaction> claimed;
	{
		lock_guard<mutex> guard(lock);
		D_ASSERT(pending_joins > 0);
		pending_joins--;
		claimed = ClaimForDestruction();
	}
	Destroy(claimed);
}

void SharedTransactionState::LeaveParticipation() {
	optional_ptr<DuckTransaction> claimed;
	{
		lock_guard<mutex> guard(lock);
		D_ASSERT(participants > 0);
		participants--;
		claimed = ClaimForDestruction();
	}
	Destroy(claimed);
}

bool SharedTransactionState::TryHandOffToParticipants() {
	lock_guard<mutex> guard(lock);
	if (participants == 0 && pending_joins == 0) {
		return false;
	}
	// Record the handoff in the same critical section that saw the other holders, so whichever of them leaves last
	// is guaranteed to see it. It is not complete yet: we still have to finish using the transaction ourselves.
	hand_off_pending = true;
	return true;
}

void SharedTransactionState::CompleteHandOff() {
	optional_ptr<DuckTransaction> claimed;
	{
		lock_guard<mutex> guard(lock);
		if (!hand_off_pending) {
			return;
		}
		hand_off_pending = false;
		owner_finished = true;
		claimed = ClaimForDestruction();
	}
	Destroy(claimed);
}

void SharedTransactionState::MarkTransactionDestroyed() {
	lock_guard<mutex> guard(lock);
	transaction = nullptr;
	destroyed = true;
}

} // namespace duckdb
