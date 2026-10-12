#include "catch.hpp"

#include "duckdb/main/http/http_retry_budget.hpp"
#include "duckdb/main/http/http_util.hpp"

using namespace duckdb;

TEST_CASE("HTTP retry back-off uses full jitter with a cap", "[http]") {
	// Retry 1 sleeps anywhere in [0, wait]
	REQUIRE(HTTPRetryBudget::BackoffMs(100, 4, 1, 0, 0) == 0);
	REQUIRE(HTTPRetryBudget::BackoffMs(100, 4, 1, 0, 100) == 100);
	REQUIRE(HTTPRetryBudget::BackoffMs(100, 4, 1, 0, 101) == 0);
	// Retry 3 grows the window to wait * backoff^2
	REQUIRE(HTTPRetryBudget::BackoffMs(100, 4, 3, 0, 1600) == 1600);
	REQUIRE(HTTPRetryBudget::BackoffMs(100, 4, 3, 0, 1601) == 0);
	// The window never exceeds the cap
	REQUIRE(HTTPRetryBudget::BackoffMs(1000, 2, 40, 0, HTTPRetryBudget::MAX_BACKOFF_MS) ==
	        HTTPRetryBudget::MAX_BACKOFF_MS);
	REQUIRE(HTTPRetryBudget::BackoffMs(1000, 2, 40, 0, HTTPRetryBudget::MAX_BACKOFF_MS + 1) == 0);
	// A Retry-After floor overrides a small jitter and is capped as well
	REQUIRE(HTTPRetryBudget::BackoffMs(100, 4, 1, 3000, 0) == 3000);
	REQUIRE(HTTPRetryBudget::BackoffMs(100, 4, 1, 60000, 0) == HTTPRetryBudget::MAX_BACKOFF_MS);
	// A zero wait never sleeps
	REQUIRE(HTTPRetryBudget::BackoffMs(0, 4, 5, 0, 12345) == 0);
}

TEST_CASE("Retry-After header values parse as seconds or HTTP-dates", "[http]") {
	uint64_t ms = 0;
	REQUIRE(HTTPRetryBudget::TryParseRetryAfterMs("3", ms));
	REQUIRE(ms == 3000);
	REQUIRE(HTTPRetryBudget::TryParseRetryAfterMs(" 7 ", ms));
	REQUIRE(ms == 7000);
	REQUIRE(HTTPRetryBudget::TryParseRetryAfterMs("3600", ms));
	REQUIRE(ms == HTTPRetryBudget::MAX_BACKOFF_MS);
	REQUIRE_FALSE(HTTPRetryBudget::TryParseRetryAfterMs("", ms));
	REQUIRE_FALSE(HTTPRetryBudget::TryParseRetryAfterMs("soon", ms));
	REQUIRE_FALSE(HTTPRetryBudget::TryParseRetryAfterMs("-5", ms));
	// A date in the past asks for no delay
	REQUIRE(HTTPRetryBudget::TryParseRetryAfterMs("Sun, 06 Nov 1994 08:49:37 GMT", ms));
	REQUIRE(ms == 0);
	// A date in the future asks for a positive delay
	REQUIRE(HTTPRetryBudget::TryParseRetryAfterMs("Fri, 31 Dec 2999 23:59:59 GMT", ms));
	REQUIRE(ms > 0);
}

TEST_CASE("HTTP retry quota charges retries and refunds successes", "[http]") {
	HTTPRetryQuota quota;
	REQUIRE(quota.Available() == HTTPRetryQuota::INITIAL_CAPACITY);
	REQUIRE(quota.TryAcquire(HTTPRetryQuota::RETRY_COST));
	REQUIRE(quota.Available() == HTTPRetryQuota::INITIAL_CAPACITY - HTTPRetryQuota::RETRY_COST);
	// Refunds never grow the quota beyond its initial capacity
	quota.Release(HTTPRetryQuota::INITIAL_CAPACITY);
	REQUIRE(quota.Available() == HTTPRetryQuota::INITIAL_CAPACITY);
	REQUIRE_FALSE(quota.TryAcquire(HTTPRetryQuota::INITIAL_CAPACITY + 1));
	REQUIRE(quota.Available() == HTTPRetryQuota::INITIAL_CAPACITY);
	REQUIRE(quota.TryAcquire(HTTPRetryQuota::INITIAL_CAPACITY));
	REQUIRE(quota.Available() == 0);
	REQUIRE_FALSE(quota.TryAcquire(1));
}

TEST_CASE("HTTP retry budget stops when the host quota is exhausted", "[http]") {
	HTTPUtil http_util;
	HTTPParams params(http_util);
	params.retries = 100;
	params.retry_wait_ms = 0;
	HTTPRetryQuota quota;
	// Leave room for exactly two server-error retries
	REQUIRE(quota.TryAcquire(HTTPRetryQuota::INITIAL_CAPACITY - 2 * HTTPRetryQuota::RETRY_COST));

	HTTPRetryBudget budget(params);
	budget.BindQuota(quota);
	idx_t attempts = 0;
	budget.Run([&]() {
		attempts++;
		return HTTPRetryDecision::Retry();
	});
	REQUIRE(attempts == 3);
	REQUIRE(budget.RetriesUsed() == 2);
	REQUIRE(budget.QuotaExhausted());
	REQUIRE(quota.Available() == 0);

	// A success refunds the cost of the last admitted retry
	budget.RecordSuccess();
	REQUIRE(quota.Available() == HTTPRetryQuota::RETRY_COST);

	// Transport failures cost more than server errors
	HTTPRetryBudget transient_budget(params);
	transient_budget.BindQuota(quota);
	attempts = 0;
	transient_budget.Run([&]() {
		attempts++;
		return HTTPRetryDecision::Transient();
	});
	REQUIRE(attempts == 1);
	REQUIRE(transient_budget.QuotaExhausted());
	quota.Release(HTTPRetryQuota::TRANSIENT_RETRY_COST);
	HTTPRetryBudget transient_budget_with_room(params);
	transient_budget_with_room.BindQuota(quota);
	attempts = 0;
	transient_budget_with_room.Run([&]() {
		attempts++;
		return HTTPRetryDecision::Transient();
	});
	REQUIRE(attempts == 2);
	REQUIRE(quota.Available() == HTTPRetryQuota::RETRY_COST);

	// A request that succeeded without retrying refunds a single token
	HTTPRetryBudget clean_budget(params);
	clean_budget.BindQuota(quota);
	clean_budget.RecordSuccess();
	REQUIRE(quota.Available() == HTTPRetryQuota::RETRY_COST + HTTPRetryQuota::NO_RETRY_REFUND);

	// Throttled retries beyond http_retries are still charged
	params.retries = 1;
	HTTPRetryBudget throttled_budget(params);
	throttled_budget.BindQuota(quota);
	attempts = 0;
	throttled_budget.Run([&]() {
		attempts++;
		return HTTPRetryDecision::Throttled("0");
	});
	REQUIRE(attempts == 2);
	REQUIRE(throttled_budget.ThrottledRetries() == 1);
	REQUIRE(quota.Available() == HTTPRetryQuota::NO_RETRY_REFUND);
}

TEST_CASE("HTTP retry budget grants extra throttled retries unless retries are off", "[http]") {
	HTTPUtil http_util;
	HTTPParams params(http_util);
	params.retry_wait_ms = 0;

	params.retries = 1;
	HTTPRetryBudget budget(params);
	idx_t attempts = 0;
	budget.Run([&]() {
		attempts++;
		return HTTPRetryDecision::Throttled();
	});
	REQUIRE(attempts == 1 + 1 + HTTPRetryBudget::THROTTLE_EXTRA_RETRIES);
	REQUIRE(budget.ThrottledRetries() == 1 + HTTPRetryBudget::THROTTLE_EXTRA_RETRIES);

	params.retries = 0;
	HTTPRetryBudget no_retries(params);
	attempts = 0;
	no_retries.Run([&]() {
		attempts++;
		return HTTPRetryDecision::Throttled();
	});
	REQUIRE(attempts == 1);
}

TEST_CASE("HTTPUtil hands out one retry quota per host", "[http]") {
	HTTPUtil http_util;
	auto &first = http_util.GetRetryQuota("https://a.example.com");
	auto &second = http_util.GetRetryQuota("https://b.example.com");
	REQUIRE(&first != &second);
	REQUIRE(&first == &http_util.GetRetryQuota("https://a.example.com"));
}
