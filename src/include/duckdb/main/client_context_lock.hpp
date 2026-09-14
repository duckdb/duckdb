//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context_lock.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class DUCKDB_CAPABILITY("mutex") DUCKDB_SCOPED_CAPABILITY ClientContextLock {
public:
	explicit ClientContextLock(ClientContext &context) DUCKDB_ACQUIRE(context.context_lock)
	    : client_guard(context.context_lock), locked_context(context) {
	}
	~ClientContextLock() DUCKDB_RELEASE() = default;

	ClientContextLock(const ClientContextLock &) = delete;
	ClientContextLock &operator=(const ClientContextLock &) = delete;

	//! Verify that a borrowed guard holds this context's mutex.
	void AssertHeld(const ClientContext &context) const DUCKDB_ASSERT_CAPABILITY(this)
	    DUCKDB_ASSERT_CAPABILITY(context.context_lock) {
		D_ASSERT(&locked_context == &context);
	}

private:
	lock_guard<mutex> client_guard;
	ClientContext &locked_context;
};

} // namespace duckdb
