//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/http/http_retry_budget.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include <functional>

namespace duckdb {

struct HTTPParams;

//! An attempt's decision; constructing one does not consume retry budget.
class HTTPRetryDecision {
	friend class HTTPRetryBudget;
	friend class HTTPUtil;

public:
	DUCKDB_API static HTTPRetryDecision Finish();
	DUCKDB_API static HTTPRetryDecision Retry();

private:
	enum class Type : uint8_t { FINISH, RETRY, THROTTLED };
	HTTPRetryDecision(Type type_p, string retry_after_p = {}) : type(type_p), retry_after(std::move(retry_after_p)) {
	}
	static HTTPRetryDecision Throttled(const string &retry_after);

private:
	Type type;
	string retry_after;
};

//! Operation-local budget shared by synchronous nested retry loops, never concurrent loops.
class HTTPRetryBudget {
	friend class HTTPUtil;

public:
	DUCKDB_API explicit HTTPRetryBudget(const HTTPParams &params);
	HTTPRetryBudget(const HTTPRetryBudget &) = delete;
	HTTPRetryBudget &operator=(const HTTPRetryBudget &) = delete;

public:
	//! Run once, then consume budget and wait before each requested retry. Exhaustion returns normally.
	//! Callbacks establish replay safety; exceptions propagate without further retries.
	DUCKDB_API void Run(const std::function<HTTPRetryDecision()> &attempt);

private:
	//! Core's hook runs after admission/backoff, outside the attempt's transport-error handling.
	void Run(const std::function<HTTPRetryDecision()> &attempt, const std::function<void()> &before_retry);
	bool ConsumeAndWait(const HTTPRetryDecision &decision);

private:
	//! Retry settings captured for the operation.
	const uint64_t retries;
	const uint64_t retry_wait_ms;
	const float retry_backoff;

	//! Retries admitted across all participating requests.
	uint64_t retries_used = 0;
};

} // namespace duckdb
