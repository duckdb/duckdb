#include "catch.hpp"

#include "duckdb/main/http/http_retry_budget.hpp"
#include "duckdb/main/http/http_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"

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

TEST_CASE("Throttled HTTP retries keep at least half the back-off window", "[http]") {
	// Retry 1 against a throttling server sleeps in [wait / 2, wait]
	REQUIRE(HTTPRetryBudget::BackoffMs(100, 4, 1, 0, 0, true) == 50);
	REQUIRE(HTTPRetryBudget::BackoffMs(100, 4, 1, 0, 50, true) == 100);
	REQUIRE(HTTPRetryBudget::BackoffMs(100, 4, 1, 0, 51, true) == 50);
	// Retry 3 keeps the proportion
	REQUIRE(HTTPRetryBudget::BackoffMs(100, 4, 3, 0, 0, true) == 800);
	REQUIRE(HTTPRetryBudget::BackoffMs(100, 4, 3, 0, 800, true) == 1600);
	// The cap applies to the whole window
	REQUIRE(HTTPRetryBudget::BackoffMs(1000, 2, 40, 0, 0, true) == HTTPRetryBudget::MAX_BACKOFF_MS / 2);
	// Retry-After still raises the floor
	REQUIRE(HTTPRetryBudget::BackoffMs(100, 4, 1, 3000, 0, true) == 3000);
	// A zero wait never sleeps, throttled or not
	REQUIRE(HTTPRetryBudget::BackoffMs(0, 4, 5, 0, 12345, true) == 0);
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

TEST_CASE("Retryable HTTP statuses are the transient ones", "[http]") {
	REQUIRE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::RequestTimeout_408));
	REQUIRE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::TooManyRequests_429));
	REQUIRE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::InternalServerError_500));
	REQUIRE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::BadGateway_502));
	REQUIRE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::ServiceUnavailable_503));
	REQUIRE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::GatewayTimeout_504));
	REQUIRE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::InsufficientStorage_507));
	// Server errors about the request or the server's abilities never clear up on their own
	REQUIRE_FALSE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::NotImplemented_501));
	REQUIRE_FALSE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::HttpVersionNotSupported_505));
	REQUIRE_FALSE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::NetworkAuthenticationRequired_511));
	// Client errors other than the timeouts and throttles are final
	REQUIRE_FALSE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::Forbidden_403));
	REQUIRE_FALSE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::NotFound_404));
	REQUIRE_FALSE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::OK_200));
	REQUIRE_FALSE(HTTPUtil::IsRetryableStatus(HTTPStatusCode::INVALID));
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

TEST_CASE("HTTP retry quota is full again after the host was left alone", "[http]") {
	HTTPRetryQuota quota;
	const auto now_ms = TimePoint::GetTickMs();
	REQUIRE(quota.TryAcquire(HTTPRetryQuota::INITIAL_CAPACITY, now_ms));
	REQUIRE(quota.Available() == 0);
	// Use shortly after keeps the quota empty
	REQUIRE_FALSE(quota.TryAcquire(1, now_ms + HTTPRetryQuota::IDLE_RESET_MS - 1));
	REQUIRE(quota.IdleMs(now_ms + HTTPRetryQuota::IDLE_RESET_MS - 1) == 0);
	// Once nothing has touched it for the idle period, the quota starts over
	REQUIRE(quota.TryAcquire(HTTPRetryQuota::RETRY_COST, now_ms + 2 * HTTPRetryQuota::IDLE_RESET_MS));
	REQUIRE(quota.Available() == HTTPRetryQuota::INITIAL_CAPACITY - HTTPRetryQuota::RETRY_COST);
}

TEST_CASE("HTTP retry budget stops when the host quota is exhausted", "[http]") {
	HTTPUtil http_util;
	HTTPParams params(http_util);
	params.retries = 100;
	params.retry_wait_ms = 0;
	auto quota = make_shared_ptr<HTTPRetryQuota>();
	// Leave room for exactly two server-error retries
	REQUIRE(quota->TryAcquire(HTTPRetryQuota::INITIAL_CAPACITY - 2 * HTTPRetryQuota::RETRY_COST));

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
	REQUIRE(quota->Available() == 0);

	// The refused retry means the earlier ones did not get the request through: a later success in the same
	// budget refunds only the single token of a request that never retried
	budget.RecordSuccess();
	REQUIRE(quota->Available() == HTTPRetryQuota::NO_RETRY_REFUND);
	quota->Release(HTTPRetryQuota::RETRY_COST - HTTPRetryQuota::NO_RETRY_REFUND);

	// A success right after an admitted retry refunds that retry's cost
	HTTPRetryBudget refunded_budget(params);
	refunded_budget.BindQuota(quota);
	attempts = 0;
	refunded_budget.Run([&]() {
		attempts++;
		return attempts == 1 ? HTTPRetryDecision::Retry() : HTTPRetryDecision::Finish();
	});
	REQUIRE(attempts == 2);
	REQUIRE(quota->Available() == 0);
	refunded_budget.RecordSuccess();
	REQUIRE(quota->Available() == HTTPRetryQuota::RETRY_COST);

	// A run that starts afresh is not marked by an earlier refusal
	HTTPRetryBudget reused_budget(params);
	reused_budget.BindQuota(quota);
	reused_budget.Run([&]() { return HTTPRetryDecision::Retry(); });
	REQUIRE(reused_budget.QuotaExhausted());
	reused_budget.Run([&]() { return HTTPRetryDecision::Finish(); });
	REQUIRE_FALSE(reused_budget.QuotaExhausted());
	REQUIRE(quota->Available() == 0);

	// Transport failures cost more than server errors
	quota->Release(HTTPRetryQuota::RETRY_COST);
	HTTPRetryBudget transient_budget(params);
	transient_budget.BindQuota(quota);
	attempts = 0;
	transient_budget.Run([&]() {
		attempts++;
		return HTTPRetryDecision::Transient();
	});
	REQUIRE(attempts == 1);
	REQUIRE(transient_budget.QuotaExhausted());
	quota->Release(HTTPRetryQuota::TRANSIENT_RETRY_COST);
	HTTPRetryBudget transient_budget_with_room(params);
	transient_budget_with_room.BindQuota(quota);
	attempts = 0;
	transient_budget_with_room.Run([&]() {
		attempts++;
		return HTTPRetryDecision::Transient();
	});
	REQUIRE(attempts == 2);
	REQUIRE(quota->Available() == HTTPRetryQuota::RETRY_COST);

	// A request that succeeded without retrying refunds a single token
	HTTPRetryBudget clean_budget(params);
	clean_budget.BindQuota(quota);
	clean_budget.RecordSuccess();
	REQUIRE(quota->Available() == HTTPRetryQuota::RETRY_COST + HTTPRetryQuota::NO_RETRY_REFUND);

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
	REQUIRE(quota->Available() == HTTPRetryQuota::NO_RETRY_REFUND);
}

TEST_CASE("HTTP retry budget charges the host of the request being retried", "[http]") {
	HTTPUtil http_util;
	HTTPParams params(http_util);
	params.retries = 10;
	params.retry_wait_ms = 0;
	auto first_host = make_shared_ptr<HTTPRetryQuota>();
	auto second_host = make_shared_ptr<HTTPRetryQuota>();

	// One operation: a request to the first host is retried once, then a redirect sends it to the second host,
	// where it is retried once more and succeeds
	HTTPRetryBudget budget(params);
	budget.BindQuota(first_host);
	idx_t attempts = 0;
	budget.Run([&]() {
		attempts++;
		return attempts == 1 ? HTTPRetryDecision::Retry() : HTTPRetryDecision::Finish();
	});
	REQUIRE(first_host->Available() == HTTPRetryQuota::INITIAL_CAPACITY - HTTPRetryQuota::RETRY_COST);
	REQUIRE(second_host->Available() == HTTPRetryQuota::INITIAL_CAPACITY);

	budget.BindQuota(second_host);
	attempts = 0;
	budget.Run([&]() {
		attempts++;
		return attempts == 1 ? HTTPRetryDecision::Retry() : HTTPRetryDecision::Finish();
	});
	REQUIRE(first_host->Available() == HTTPRetryQuota::INITIAL_CAPACITY - HTTPRetryQuota::RETRY_COST);
	REQUIRE(second_host->Available() == HTTPRetryQuota::INITIAL_CAPACITY - HTTPRetryQuota::RETRY_COST);

	// The success refunds the host that was charged for the last retry
	budget.RecordSuccess();
	REQUIRE(first_host->Available() == HTTPRetryQuota::INITIAL_CAPACITY - HTTPRetryQuota::RETRY_COST);
	REQUIRE(second_host->Available() == HTTPRetryQuota::INITIAL_CAPACITY);
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
	// Every attempt stayed within what the budget announces as its maximum
	REQUIRE(attempts == budget.MaxAttempts());

	params.retries = 0;
	HTTPRetryBudget no_retries(params);
	attempts = 0;
	no_retries.Run([&]() {
		attempts++;
		return HTTPRetryDecision::Throttled();
	});
	REQUIRE(attempts == 1);
	REQUIRE(no_retries.MaxAttempts() == 1);
}

TEST_CASE("HTTP retry budget numbers every send of the operation", "[http]") {
	HTTPUtil http_util;
	HTTPParams params(http_util);
	params.retries = 3;
	params.retry_wait_ms = 0;
	HTTPRetryBudget budget(params);
	REQUIRE(budget.MaxAttempts() == 1 + 3 + HTTPRetryBudget::THROTTLE_EXTRA_RETRIES);
	// Two sends in one inner loop, a corrected re-send without a retry in between, then one more
	REQUIRE(budget.BeginAttempt() == 1);
	REQUIRE(budget.BeginAttempt() == 2);
	REQUIRE(budget.BeginAttempt() == 3);
	REQUIRE(budget.RetriesUsed() == 0);
	budget.Run([&]() { return HTTPRetryDecision::Finish(); });
	REQUIRE(budget.BeginAttempt() == 4);
}

namespace {

unique_ptr<HTTPResponse> MakeResponse(HTTPStatusCode status, const string &body = string()) {
	auto response = make_uniq<HTTPResponse>(status);
	response->body = body;
	return response;
}

const char *S3_REQUEST_TIMEOUT_BODY = "<Error><Code>RequestTimeout</Code><Message>Your socket connection to the "
                                      "server was not read from or written to within the timeout period.</Message>"
                                      "</Error>";
const char *S3_INTERNAL_ERROR_BODY = "<Error><Code>InternalError</Code><Message>We encountered an internal error. "
                                     "Please try again.</Message></Error>";

} // namespace

TEST_CASE("An S3-style 400 RequestTimeout is retried whether it was received or thrown", "[http]") {
	HTTPUtil http_util;
	HTTPParams params(http_util);
	params.retries = 2;
	params.retry_wait_ms = 0;
	HTTPHeaders headers;

	HTTPResponse timeout(HTTPStatusCode::BadRequest_400);
	timeout.body = S3_REQUEST_TIMEOUT_BODY;
	REQUIRE(HTTPUtil::IsRequestTimeoutResponse(timeout));
	REQUIRE(timeout.ShouldRetry());
	// Other 400s and the same code behind another status are left alone
	HTTPResponse malformed(HTTPStatusCode::BadRequest_400);
	malformed.body = "<Error><Code>MalformedXML</Code></Error>";
	REQUIRE_FALSE(HTTPUtil::IsRequestTimeoutResponse(malformed));
	REQUIRE_FALSE(malformed.ShouldRetry());
	HTTPResponse forbidden(HTTPStatusCode::Forbidden_403);
	forbidden.body = S3_REQUEST_TIMEOUT_BODY;
	REQUIRE_FALSE(HTTPUtil::IsRequestTimeoutResponse(forbidden));

	// A received RequestTimeout uses the retries of the request
	GetRequestInfo received(
	    "http://bucket.s3.amazonaws.com/key", headers, params, [](const HTTPResponse &) { return true; },
	    [](const_data_ptr_t, idx_t) { return true; });
	received.try_request = true;
	idx_t sends = 0;
	auto response = HTTPUtil::RunRequestWithRetry(
	    [&]() {
		    sends++;
		    return MakeResponse(HTTPStatusCode::BadRequest_400, S3_REQUEST_TIMEOUT_BODY);
	    },
	    received, {});
	REQUIRE(sends == 3);
	REQUIRE(response->status == HTTPStatusCode::BadRequest_400);
	REQUIRE_FALSE(response->Success());

	// A handler that turns the RequestTimeout into an exception is retried just the same
	GetRequestInfo thrown(
	    "http://bucket.s3.amazonaws.com/key", headers, params, [](const HTTPResponse &) { return true; },
	    [](const_data_ptr_t, idx_t) { return true; });
	thrown.try_request = true;
	sends = 0;
	response = HTTPUtil::RunRequestWithRetry(
	    [&]() -> unique_ptr<HTTPResponse> {
		    sends++;
		    auto failed = MakeResponse(HTTPStatusCode::BadRequest_400, S3_REQUEST_TIMEOUT_BODY);
		    throw HTTPException(*failed, "HTTP GET error reading '%s' (HTTP 400 Bad Request)", thrown.url);
	    },
	    thrown, {});
	REQUIRE(sends == 3);
	REQUIRE(response->HasRequestError());

	// A thrown 400 without the timeout code stays final
	GetRequestInfo final_error(
	    "http://bucket.s3.amazonaws.com/key", headers, params, [](const HTTPResponse &) { return true; },
	    [](const_data_ptr_t, idx_t) { return true; });
	final_error.try_request = true;
	sends = 0;
	response = HTTPUtil::RunRequestWithRetry(
	    [&]() -> unique_ptr<HTTPResponse> {
		    sends++;
		    auto failed = MakeResponse(HTTPStatusCode::BadRequest_400, "<Error><Code>MalformedXML</Code></Error>");
		    throw HTTPException(*failed, "HTTP GET error reading '%s' (HTTP 400 Bad Request)", final_error.url);
	    },
	    final_error, {});
	REQUIRE(sends == 1);
	REQUIRE(response->HasRequestError());
}

TEST_CASE("HTTPUtil hands out one retry quota per host and forgets the quietest", "[http]") {
	HTTPUtil http_util;
	auto first = http_util.GetRetryQuota("https://a.example.com");
	auto second = http_util.GetRetryQuota("https://b.example.com");
	REQUIRE(first != second);
	REQUIRE(first == http_util.GetRetryQuota("https://a.example.com"));

	// Make the first host the one that was used least recently
	REQUIRE(first->TryAcquire(HTTPRetryQuota::RETRY_COST, TimePoint::GetTickMs() - 1000));
	for (idx_t i = 0; i < HTTPUtil::MAX_RETRY_QUOTAS - 2; i++) {
		http_util.GetRetryQuota(StringUtil::Format("https://host%llu.example.com", i));
	}
	// The map is full: the next new host replaces the first one, which gets a fresh quota on its return
	auto newcomer = http_util.GetRetryQuota("https://c.example.com");
	REQUIRE(newcomer != first);
	REQUIRE(second == http_util.GetRetryQuota("https://b.example.com"));
	auto returned = http_util.GetRetryQuota("https://a.example.com");
	REQUIRE(returned != first);
	REQUIRE(returned->Available() == HTTPRetryQuota::INITIAL_CAPACITY);
	// The budget that still held the evicted quota keeps a valid object
	REQUIRE(first->Available() == HTTPRetryQuota::INITIAL_CAPACITY - HTTPRetryQuota::RETRY_COST);
}
