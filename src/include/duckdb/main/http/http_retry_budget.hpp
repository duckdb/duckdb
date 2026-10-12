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
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/time_point.hpp"
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
//! A host nobody has talked to for a while starts over with a full quota, like the CRT drops an idle host's bucket.
class HTTPRetryQuota {
public:
	static constexpr int64_t INITIAL_CAPACITY = 500;
	//! Cost of retrying a received server error or throttle: the server answered, it is just unhappy.
	static constexpr int64_t RETRY_COST = 5;
	//! Cost of retrying a transport failure: nothing is known about the server, so it costs more.
	static constexpr int64_t TRANSIENT_RETRY_COST = 10;
	//! Refund for a request that succeeded without retrying.
	static constexpr int64_t NO_RETRY_REFUND = 1;
	//! A quota untouched for this long is full again when it is next used.
	static constexpr int64_t IDLE_RESET_MS = 60 * 1000;

public:
	//! Take `cost` tokens; false (and nothing taken) when fewer are available.
	DUCKDB_API bool TryAcquire(int64_t cost, int64_t now_ms = TimePoint::GetTickMs());
	//! Return tokens, never above the initial capacity.
	DUCKDB_API void Release(int64_t amount, int64_t now_ms = TimePoint::GetTickMs());
	DUCKDB_API int64_t Available() const;
	//! Milliseconds since the quota was last charged or refunded.
	DUCKDB_API int64_t IdleMs(int64_t now_ms = TimePoint::GetTickMs()) const;

private:
	//! Refill an idle quota and record the use; the lock must be held.
	void Touch(int64_t now_ms);

private:
	mutable mutex lock;
	int64_t capacity = INITIAL_CAPACITY;
	int64_t last_used_ms = TimePoint::GetTickMs();
};

//! Operation-local budget shared by synchronous nested retry loops, never concurrent loops. One budget is one
//! logical operation (one `amz-sdk-invocation-id`): every send it makes, including corrected re-sends, is one of
//! its attempts, as in the AWS SDK's attempt loop.
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
	//! Number the next send of the operation: the first call returns 1.
	DUCKDB_API uint64_t BeginAttempt();
	//! Most attempts the operation can make: the first one, `http_retries`, and the throttle extras.
	DUCKDB_API uint64_t MaxAttempts() const;
	//! Whether the current Run refused a retry because the host's quota ran out.
	DUCKDB_API bool QuotaExhausted() const;
	//! Charge the next admitted retries to this host's quota; a request to another host rebinds.
	DUCKDB_API void BindQuota(shared_ptr<HTTPRetryQuota> quota);
	//! Refund the last admitted retry to the quota it was charged to, or NO_RETRY_REFUND to the bound quota.
	DUCKDB_API void RecordSuccess();
	//! Forget the last admitted retry: the request it was spent on failed, so a later success does not refund it.
	DUCKDB_API void RecordFailure();

	//! Back-off window for `retry` is [0, min(wait * backoff^(retry - 1), MAX_BACKOFF_MS)]: full jitter, or its upper
	//! half for throttled retries so they always keep some distance. The result is raised to `floor_ms`.
	DUCKDB_API static uint64_t BackoffMs(uint64_t retry_wait_ms, float retry_backoff, uint64_t retry, uint64_t floor_ms,
	                                     uint64_t random_value, bool throttled = false);
	//! Parse a Retry-After header (delay in seconds or an HTTP-date) into a delay in milliseconds.
	DUCKDB_API static bool TryParseRetryAfterMs(const string &retry_after, uint64_t &delay_ms);

private:
	//! Core's hook runs after admission/backoff, outside the attempt's transport-error handling.
	void Run(const std::function<HTTPRetryDecision()> &attempt, const std::function<void()> &before_retry);
	bool ConsumeAndWait(const HTTPRetryDecision &decision);
	uint64_t ThrottleExtraRetries() const;

private:
	//! Retry settings captured for the operation.
	const uint64_t retries;
	const uint64_t retry_wait_ms;
	const float retry_backoff;

	//! Retries admitted across all participating requests.
	uint64_t retries_used = 0;
	uint64_t throttled_retries = 0;
	//! Sends numbered so far.
	uint64_t attempts = 0;
	//! Quota of the host the current request goes to, if any.
	shared_ptr<HTTPRetryQuota> quota;
	//! Quota and tokens of the most recent admitted retry, refunded on success.
	shared_ptr<HTTPRetryQuota> last_retry_quota;
	int64_t last_retry_cost = 0;
	bool quota_exhausted = false;
};

} // namespace duckdb
