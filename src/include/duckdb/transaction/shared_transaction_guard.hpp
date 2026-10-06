//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/shared_transaction_guard.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class ClientContext;
class SharedTransactionLock;

//! How a caller takes hold of a shared transaction's statement lock.
enum class SharedTransactionGuardMode : uint8_t {
	//! Read alongside the owner and other participants, excluding writes and finalization.
	ACQUIRE_SHARED,
	//! Exclude every other connection taking part.
	ACQUIRE_EXCLUSIVE,
	//! Take over the exclusive lock that ShareTransaction acquired on this statement's behalf.
	ADOPT_EXCLUSIVE
};

//! Whether waiting for the statement lock can be given up on.
enum class SharedTransactionGuardWait : uint8_t {
	//! Abort the wait when the query is interrupted or exceeds max_execution_time.
	INTERRUPTIBLE,
	//! Wait unconditionally, for callers with no way to report a failure, such as connection teardown.
	UNINTERRUPTIBLE
};

//! Holds a shared transaction's statement lock, and counts itself on the connection holding it, for as long as it
//! is alive. Readers take it shared; the owner takes it exclusively for writes and finalization.
class SharedTransactionGuard {
public:
	//! Acquire the statement lock. Waiting checks for interrupts and the query deadline unless told otherwise.
	SharedTransactionGuard(ClientContext &context, shared_ptr<SharedTransactionLock> statement_lock,
	                       SharedTransactionGuardMode mode,
	                       SharedTransactionGuardWait wait = SharedTransactionGuardWait::INTERRUPTIBLE);
	~SharedTransactionGuard();
	SharedTransactionGuard(const SharedTransactionGuard &) = delete;
	SharedTransactionGuard &operator=(const SharedTransactionGuard &) = delete;

public:
	//! The lock being held, so a caller can tell whether it already holds the one it needs.
	const shared_ptr<SharedTransactionLock> &GetStatementLock() const {
		return statement_lock;
	}
	//! Whether the lock is held exclusively. An exclusive hold also covers a shared request.
	bool IsExclusive() const {
		return exclusive;
	}

private:
	ClientContext &context;
	shared_ptr<SharedTransactionLock> statement_lock;
	bool exclusive;
};

} // namespace duckdb
