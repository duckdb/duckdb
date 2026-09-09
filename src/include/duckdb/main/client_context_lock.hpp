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

namespace duckdb {

class DUCKDB_CAPABILITY("mutex") DUCKDB_SCOPED_CAPABILITY ClientContextLock {
public:
	explicit ClientContextLock(annotated_mutex &context_lock) DUCKDB_ACQUIRE(context_lock)
	    : client_guard(context_lock), locked_mutex(context_lock) {
	}
	~ClientContextLock() DUCKDB_RELEASE() = default;

	ClientContextLock(const ClientContextLock &) = delete;
	ClientContextLock &operator=(const ClientContextLock &) = delete;

	//! Verify that a borrowed guard holds this context's mutex.
	void AssertHeld(const annotated_mutex &context_lock) const DUCKDB_ASSERT_CAPABILITY(this)
	    DUCKDB_ASSERT_CAPABILITY(context_lock) {
		D_ASSERT(&locked_mutex == &context_lock);
	}

private:
	lock_guard<mutex> client_guard;
	annotated_mutex &locked_mutex;
};

} // namespace duckdb
