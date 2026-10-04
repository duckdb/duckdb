//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/shared_transaction_lock.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/condition_variable.hpp"

#include <functional>

namespace duckdb {

//! Statement lock of a shared transaction (see duckdb_export_transaction_snapshot / SET TRANSACTION SNAPSHOT).
//!
//! A DuckTransaction's undo buffer and LocalStorage are built for a single writer and are not safe to mutate while
//! another connection reads them. Participants only read. The owner can read alongside them, but takes this lock
//! exclusively before modifying or finalizing the transaction.
//!
//! It is held per statement, not per query operator: a single statement holds it once for its whole duration, so
//! intra-query parallelism inside that statement is unaffected. The owner's connection close also takes it
//! exclusively, so a transaction is never torn down underneath a participant that is mid-read.
//!
//! Waiting writers block new readers, so a stream of participant reads cannot starve the owner. Acquire and
//! release may happen on different threads, because a statement's guard is taken on whichever thread begins the
//! query and dropped when the query ends.
class SharedTransactionLock {
public:
	//! Invoked every few milliseconds while waiting; throwing abandons the wait.
	using WaitCheck = std::function<void()>;

	void LockExclusive(const WaitCheck &wait_check = WaitCheck()) {
		Acquire(true, wait_check);
	}
	void LockShared(const WaitCheck &wait_check = WaitCheck()) {
		Acquire(false, wait_check);
	}

	bool TryLockExclusiveFor(const std::chrono::milliseconds &timeout) {
		unique_lock<mutex> guard(lock);
		waiting_writers++;
		bool acquired = condition.wait_for(guard, timeout, [&]() { return CanLockExclusive(); });
		waiting_writers--;
		if (acquired) {
			writer = true;
		} else {
			guard.unlock();
			condition.notify_all();
		}
		return acquired;
	}

	bool TryLockSharedFor(const std::chrono::milliseconds &timeout) {
		unique_lock<mutex> guard(lock);
		if (!condition.wait_for(guard, timeout, [&]() { return CanLockShared(); })) {
			return false;
		}
		readers++;
		return true;
	}

	void UnlockExclusive() {
		{
			lock_guard<mutex> guard(lock);
			D_ASSERT(writer);
			writer = false;
		}
		condition.notify_all();
	}

	void UnlockShared() {
		{
			lock_guard<mutex> guard(lock);
			D_ASSERT(readers > 0);
			readers--;
		}
		condition.notify_all();
	}

private:
	bool CanLockExclusive() const {
		return !writer && readers == 0;
	}
	//! Waiting writers take precedence so that a stream of readers cannot starve the owner.
	bool CanLockShared() const {
		return !writer && waiting_writers == 0;
	}
	bool CanLock(bool exclusive) const {
		return exclusive ? CanLockExclusive() : CanLockShared();
	}

	void Acquire(bool exclusive, const WaitCheck &wait_check) {
		unique_lock<mutex> guard(lock);
		if (exclusive) {
			waiting_writers++;
		}
		try {
			while (!CanLock(exclusive)) {
				if (!wait_check) {
					condition.wait(guard);
					continue;
				}
				condition.wait_for(guard, std::chrono::milliseconds(10));
				if (!CanLock(exclusive)) {
					wait_check();
				}
			}
		} catch (...) {
			if (exclusive) {
				waiting_writers--;
				guard.unlock();
				condition.notify_all();
			}
			throw;
		}
		if (exclusive) {
			waiting_writers--;
			writer = true;
		} else {
			readers++;
		}
	}

private:
	mutex lock;
	condition_variable condition;
	idx_t readers = 0;
	bool writer = false;
	idx_t waiting_writers = 0;
};

} // namespace duckdb
