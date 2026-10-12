//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/http/http_retry_budget.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include <functional>

namespace duckdb {

struct HTTPParams;

//! An attempt's decision; constructing one does not consume retry budget.
class HTTPRetryDecision {
	friend class HTTPRetryBudget;
	friend class HTTPUtil;

public:
	DUCKDB_API static HTTPRetryDecision Finish();
	//! Retry after a received server error (e.g. HTTP 500).
	DUCKDB_API static HTTPRetryDecision Retry();
	//! Retry after a transport failure (connection error, timeout, truncated body): no response was received.
	DUCKDB_API static HTTPRetryDecision Transient();
	//! Retry after a throttling response (HTTP 429/503), honoring an optional Retry-After header value.
	DUCKDB_API static HTTPRetryDecision Throttled(const string &retry_after = string());

private:
	enum class Type : uint8_t { FINISH, RETRY, TRANSIENT, THROTTLED };
	explicit HTTPRetryDecision(Type type_p, string retry_after_p = {})
	    : type(type_p), retry_after(std::move(retry_after_p)) {
	}

private:
	Type type;
	string retry_after;
};

//! Retry tokens shared by every operation against one host, after the AWS SDK retry quota.
//! Retries spend tokens and successes refund them, so a host that keeps failing stops being retried.
class HTTPRetryQuota {
public:
	static constexpr int64_t INITIAL_CAPACITY = 500;
	//! Cost of retrying a received server error or throttle: the server answered, it is just unhappy.
	static constexpr int64_t RETRY_COST = 5;
	//! Cost of retrying a transport failure: nothing is known about the server, so it costs more.
	static constexpr int64_t TRANSIENT_RETRY_COST = 10;
	//! Refund for a request that succeeded without retrying.
	static constexpr int64_t NO_RETRY_REFUND = 1;

public:
	//! Take `cost` tokens; false (and nothing taken) when fewer are available.
	DUCKDB_API bool TryAcquire(int64_t cost);
	//! Return tokens, never above the initial capacity.
	DUCKDB_API void Release(int64_t amount);
	DUCKDB_API int64_t Available() const;

private:
	mutable mutex lock;
	int64_t capacity = INITIAL_CAPACITY;
};

//! Operation-local budget shared by synchronous nested retry loops, never concurrent loops.
class HTTPRetryBudget {
	friend class HTTPUtil;

public:
	//! Longest single back-off sleep, as in the AWS SDKs.
	static constexpr uint64_t MAX_BACKOFF_MS = 20000;
	//! Retries granted beyond `http_retries` for throttled responses: the server asked to slow down, not to stop.
	static constexpr uint64_t THROTTLE_EXTRA_RETRIES = 5;

public:
	DUCKDB_API explicit HTTPRetryBudget(const HTTPParams &params);
	HTTPRetryBudget(const HTTPRetryBudget &) = delete;
	HTTPRetryBudget &operator=(const HTTPRetryBudget &) = delete;

public:
	//! Run once, then consume budget and wait before each requested retry. Exhaustion returns normally.
	//! Callbacks establish replay safety; exceptions propagate without further retries.
	DUCKDB_API void Run(const std::function<HTTPRetryDecision()> &attempt);
	//! Retries admitted for HTTP 429 or 503 responses.
	DUCKDB_API uint64_t ThrottledRetries() const;
	//! Retries admitted across all participating requests.
	DUCKDB_API uint64_t RetriesUsed() const;
	//! Whether a retry was refused because the host's quota ran out.
	DUCKDB_API bool QuotaExhausted() const;
	//! Charge admitted retries to the host's quota; only the first bound quota is kept.
	DUCKDB_API void BindQuota(HTTPRetryQuota &quota);
	//! Refund the last admitted retry (or NO_RETRY_REFUND) after a successful response.
	DUCKDB_API void RecordSuccess();

	//! Full-jitter back-off: uniform in [0, min(wait * backoff^(retry - 1), MAX_BACKOFF_MS)], raised to `floor_ms`.
	DUCKDB_API static uint64_t BackoffMs(uint64_t retry_wait_ms, float retry_backoff, uint64_t retry, uint64_t floor_ms,
	                                     uint64_t random_value);
	//! Parse a Retry-After header (delay in seconds or an HTTP-date) into a delay in milliseconds.
	DUCKDB_API static bool TryParseRetryAfterMs(const string &retry_after, uint64_t &delay_ms);

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
	uint64_t throttled_retries = 0;
	//! Quota the admitted retries are charged to, if any.
	optional_ptr<HTTPRetryQuota> quota;
	//! Tokens taken for the most recent admitted retry, refunded on success.
	int64_t last_retry_cost = 0;
	bool quota_exhausted = false;
};

} // namespace duckdb
