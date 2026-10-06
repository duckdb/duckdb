#include "duckdb/transaction/shared_transaction_guard.hpp"

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/shared_transaction_lock.hpp"

namespace duckdb {

SharedTransactionGuard::SharedTransactionGuard(ClientContext &context_p,
                                               shared_ptr<SharedTransactionLock> statement_lock_p,
                                               SharedTransactionGuardMode mode, SharedTransactionGuardWait wait)
    : context(context_p), statement_lock(std::move(statement_lock_p)),
      exclusive(mode != SharedTransactionGuardMode::ACQUIRE_SHARED) {
	if (mode != SharedTransactionGuardMode::ADOPT_EXCLUSIVE) {
		SharedTransactionLock::WaitCheck wait_check;
		if (wait == SharedTransactionGuardWait::INTERRUPTIBLE) {
			// InterruptCheck only samples the clock every N calls, so check the deadline on every poll.
			wait_check = [&context_p]() {
				context_p.InterruptCheck();
				if (!context_p.query_deadline.IsValid()) {
					return;
				}
				auto now = NumericCast<idx_t>(
				    duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
				        .count());
				if (now >= context_p.query_deadline.GetIndex()) {
					throw InterruptException("Query exceeded maximum execution time");
				}
			};
		}
		if (exclusive) {
			statement_lock->LockExclusive(wait_check);
		} else {
			statement_lock->LockShared(wait_check);
		}
	}
	// Count the hold only once the lock is genuinely held. Acquiring above can throw, and a constructor that
	// throws gets no destructor to undo this.
	context.AddSharedTransactionGuard();
}

SharedTransactionGuard::~SharedTransactionGuard() {
	context.RemoveSharedTransactionGuard();
	if (exclusive) {
		statement_lock->UnlockExclusive();
	} else {
		statement_lock->UnlockShared();
	}
}

} // namespace duckdb
