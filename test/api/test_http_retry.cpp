#include "catch.hpp"
#include "duckdb/common/http_util.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

//! Holds the objects a BaseRequest borrows, so a test can talk to the retry policy without a server
struct RetryFixture {
	RetryFixture() : params(http_util), request("http://example.com/file", headers, params) {
	}

	HTTPUtil http_util;
	HTTPHeaders headers;
	HTTPParams params;
	HeadRequestInfo request;
};

HTTPAttempt ResponseAttempt(HTTPStatusCode status) {
	HTTPAttempt attempt;
	attempt.response = make_uniq<HTTPResponse>(status);
	return attempt;
}

//! An attempt that failed without producing a response, which is always retried
HTTPAttempt ErroredAttempt() {
	HTTPAttempt attempt;
	attempt.exception_error = "connection reset";
	return attempt;
}

} // namespace

TEST_CASE("HTTP retry policy finishes on success", "[api]") {
	RetryFixture fixture;
	HTTPRetryState state;
	uint64_t delay_ms = 42;

	auto attempt = ResponseAttempt(HTTPStatusCode::OK_200);
	REQUIRE(state.OnAttempt(fixture.request, attempt, delay_ms) == HTTPRetryDecision::FINISHED);
	REQUIRE(attempt.response->success);
	REQUIRE(delay_ms == 0);
}

TEST_CASE("HTTP retry policy treats 304 as a non-retryable success", "[api]") {
	RetryFixture fixture;
	HTTPRetryState state;
	uint64_t delay_ms = 0;

	auto attempt = ResponseAttempt(HTTPStatusCode::NotModified_304);
	REQUIRE(state.OnAttempt(fixture.request, attempt, delay_ms) == HTTPRetryDecision::FINISHED);
	REQUIRE(attempt.response->success);
}

TEST_CASE("HTTP retry policy does not retry a plain client error", "[api]") {
	RetryFixture fixture;
	HTTPRetryState state;
	uint64_t delay_ms = 0;

	auto attempt = ResponseAttempt(HTTPStatusCode::NotFound_404);
	REQUIRE(state.OnAttempt(fixture.request, attempt, delay_ms) == HTTPRetryDecision::FINISHED);
	REQUIRE(!attempt.response->success);
}

TEST_CASE("HTTP retry policy stops after the configured number of retries", "[api]") {
	RetryFixture fixture;
	fixture.params.retries = 3;
	HTTPRetryState state;
	uint64_t delay_ms = 0;

	// a 500 is retryable, so we should get exactly `retries` RETRY decisions before FAILED
	for (idx_t i = 0; i < 3; i++) {
		auto attempt = ResponseAttempt(HTTPStatusCode::InternalServerError_500);
		REQUIRE(state.OnAttempt(fixture.request, attempt, delay_ms) == HTTPRetryDecision::RETRY);
	}
	auto attempt = ResponseAttempt(HTTPStatusCode::InternalServerError_500);
	REQUIRE(state.OnAttempt(fixture.request, attempt, delay_ms) == HTTPRetryDecision::FAILED);
}

TEST_CASE("HTTP retry policy backs off exponentially", "[api]") {
	RetryFixture fixture;
	fixture.params.retries = 4;
	fixture.params.retry_wait_ms = 100;
	fixture.params.retry_backoff = 4;
	HTTPRetryState state;

	// the first retry is immediate, later ones grow by retry_backoff each time
	uint64_t first = 1;
	auto attempt1 = ResponseAttempt(HTTPStatusCode::InternalServerError_500);
	REQUIRE(state.OnAttempt(fixture.request, attempt1, first) == HTTPRetryDecision::RETRY);
	REQUIRE(first == 0);

	uint64_t second = 0;
	auto attempt2 = ResponseAttempt(HTTPStatusCode::InternalServerError_500);
	REQUIRE(state.OnAttempt(fixture.request, attempt2, second) == HTTPRetryDecision::RETRY);
	REQUIRE(second == 100);

	uint64_t third = 0;
	auto attempt3 = ResponseAttempt(HTTPStatusCode::InternalServerError_500);
	REQUIRE(state.OnAttempt(fixture.request, attempt3, third) == HTTPRetryDecision::RETRY);
	REQUIRE(third == 400);
}

TEST_CASE("HTTP retry policy always retries an attempt that produced no response", "[api]") {
	RetryFixture fixture;
	fixture.params.retries = 1;
	HTTPRetryState state;
	uint64_t delay_ms = 0;

	auto attempt = ErroredAttempt();
	REQUIRE(state.OnAttempt(fixture.request, attempt, delay_ms) == HTTPRetryDecision::RETRY);
	auto second = ErroredAttempt();
	REQUIRE(state.OnAttempt(fixture.request, second, delay_ms) == HTTPRetryDecision::FAILED);
}

TEST_CASE("HTTP retry policy jitters a throttled retry within its cap", "[api]") {
	RetryFixture fixture;
	fixture.params.retries = 1;
	fixture.params.retry_wait_ms = 1000;
	fixture.params.retry_backoff = 1;
	HTTPRetryState state;
	uint64_t delay_ms = 0;

	// a 429 backs off from the very first retry, and the jitter keeps it inside [base/2, base]
	auto attempt = ResponseAttempt(HTTPStatusCode::TooManyRequests_429);
	REQUIRE(state.OnAttempt(fixture.request, attempt, delay_ms) == HTTPRetryDecision::RETRY);
	REQUIRE(delay_ms <= 1000);
	REQUIRE(delay_ms >= 500);
}

TEST_CASE("HTTP retry policy honors a numeric Retry-After", "[api]") {
	RetryFixture fixture;
	fixture.params.retries = 1;
	fixture.params.retry_wait_ms = 1;
	fixture.params.retry_backoff = 1;
	HTTPRetryState state;
	uint64_t delay_ms = 0;

	// Retry-After raises the delay well above the configured backoff, jitter can halve it at most
	auto attempt = ResponseAttempt(HTTPStatusCode::TooManyRequests_429);
	attempt.response->headers.Insert("Retry-After", "5");
	REQUIRE(state.OnAttempt(fixture.request, attempt, delay_ms) == HTTPRetryDecision::RETRY);
	REQUIRE(delay_ms >= 2500);
	REQUIRE(delay_ms <= 5000);
}

TEST_CASE("HTTP retry policy caps a very large Retry-After", "[api]") {
	RetryFixture fixture;
	fixture.params.retries = 1;
	HTTPRetryState state;
	uint64_t delay_ms = 0;

	auto attempt = ResponseAttempt(HTTPStatusCode::ServiceUnavailable_503);
	attempt.response->headers.Insert("Retry-After", "100000");
	REQUIRE(state.OnAttempt(fixture.request, attempt, delay_ms) == HTTPRetryDecision::RETRY);
	// the cap is 10s, and jitter only subtracts
	REQUIRE(delay_ms <= 10000);
}

TEST_CASE("HTTP retry policy returns a failed response for a try request", "[api]") {
	RetryFixture fixture;
	fixture.request.try_request = true;
	HTTPRetryState state;

	auto attempt = ResponseAttempt(HTTPStatusCode::InternalServerError_500);
	auto response = state.Finalize(fixture.request, attempt);
	REQUIRE(response);
	REQUIRE(!response->success);
}

TEST_CASE("HTTP retry policy throws for a non-try request", "[api]") {
	RetryFixture fixture;
	HTTPRetryState state;

	auto attempt = ResponseAttempt(HTTPStatusCode::InternalServerError_500);
	REQUIRE_THROWS(state.Finalize(fixture.request, attempt));
}

namespace {

//! The verb boilerplate both test clients need, none of which is what is under test
class StubClient : public HTTPClient {
public:
	void Initialize(HTTPParams &) override {
	}
	unique_ptr<HTTPResponse> Get(GetRequestInfo &) override {
		auto response = make_uniq<HTTPResponse>(HTTPStatusCode::OK_200);
		response->success = true;
		return response;
	}
	unique_ptr<HTTPResponse> Put(PutRequestInfo &) override {
		throw NotImplementedException("PUT");
	}
	unique_ptr<HTTPResponse> Head(HeadRequestInfo &) override {
		throw NotImplementedException("HEAD");
	}
	unique_ptr<HTTPResponse> Delete(DeleteRequestInfo &) override {
		throw NotImplementedException("DELETE");
	}
	unique_ptr<HTTPResponse> Post(PostRequestInfo &) override {
		throw NotImplementedException("POST");
	}
	unique_ptr<HTTPResponse> Options(OptionsRequestInfo &) override {
		throw NotImplementedException("OPTIONS");
	}

};

//! A client with no asynchronous transport, i.e. every backend that exists today. It does not
//! override Send, so it inherits the synchronous default.
class SynchronousClient : public StubClient {};

//! A client that can hand a request off instead of answering it, so a test can exercise both modes
class DeferringClient : public StubClient {
public:
	HTTPRequestState Send(BaseRequest &request, HTTPExecutionMode mode, HTTPResponseCallback on_complete) override {
		if (mode == HTTPExecutionMode::BLOCKING) {
			return HTTPClient::Send(request, mode, std::move(on_complete));
		}
		pending = std::move(on_complete);
		return HTTPRequestState::PENDING;
	}

	//! Deliver the deferred response, standing in for a transport completing out of band
	void Complete() {
		auto response = make_uniq<HTTPResponse>(HTTPStatusCode::OK_200);
		response->success = true;
		auto callback = std::move(pending);
		pending = nullptr;
		callback(std::move(response), nullptr);
	}

private:
	HTTPResponseCallback pending;
};

} // namespace

TEST_CASE("HTTP client without an async transport completes inline", "[api]") {
	RetryFixture fixture;
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, fixture.params, nullptr, nullptr);
	SynchronousClient client;

	idx_t completions = 0;
	auto state = client.Send(request, HTTPExecutionMode::DEFERRABLE,
	                         [&](unique_ptr<HTTPResponse> response, optional_ptr<ErrorData> error) {
		                         completions++;
		                         REQUIRE(response);
		                         REQUIRE(!error);
	                         });
	// the default inherits the synchronous path, so even DEFERRABLE finishes before returning
	REQUIRE(state == HTTPRequestState::COMPLETED);
	REQUIRE(completions == 1);
}

TEST_CASE("HTTP client defers a request only when the caller allows it", "[api]") {
	RetryFixture fixture;
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, fixture.params, nullptr, nullptr);
	DeferringClient client;

	idx_t completions = 0;
	auto on_complete = [&](unique_ptr<HTTPResponse> response, optional_ptr<ErrorData> error) {
		completions++;
		REQUIRE(response);
		REQUIRE(!error);
	};

	// BLOCKING must never be handed a result that is not ready
	REQUIRE(client.Send(request, HTTPExecutionMode::BLOCKING, on_complete) == HTTPRequestState::COMPLETED);
	REQUIRE(completions == 1);

	// DEFERRABLE lets it hand the request off, and the completion arrives later
	REQUIRE(client.Send(request, HTTPExecutionMode::DEFERRABLE, on_complete) == HTTPRequestState::PENDING);
	REQUIRE(completions == 1);
	client.Complete();
	REQUIRE(completions == 2);
}
