#include "catch.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/http_util.hpp"
#include "test_helpers.hpp"

#include <thread>

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
		callback(std::move(response), ErrorData());
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
	auto state =
	    client.Send(request, HTTPExecutionMode::DEFERRABLE, [&](unique_ptr<HTTPResponse> response, ErrorData error) {
		    completions++;
		    REQUIRE(response);
		    REQUIRE(!error.HasError());
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
	auto on_complete = [&](unique_ptr<HTTPResponse> response, ErrorData error) {
		completions++;
		REQUIRE(response);
		REQUIRE(!error.HasError());
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

namespace {

//! Models a transport that always defers and fails a fixed number of times before succeeding.
//! The state lives on the util rather than the client because core refreshes the client between
//! retries, exactly as the synchronous path does, so any per-client counter would be thrown away.
class NonBlockingUtil : public HTTPUtil {
public:
	NonBlockingUtil(idx_t failures, HTTPStatusCode failure_status)
	    : failure_status(failure_status), failures_left(failures) {
	}

	unique_ptr<HTTPClient> InitializeClient(HTTPParams &http_params, const string &proto_host_port) override;

	void Wait(uint64_t delay_ms, std::function<void()> resume) override {
		waits++;
		last_delay_ms = delay_ms;
		scheduled = std::move(resume);
	}

	//! Run whatever the retry asked us to come back to, standing in for a timer firing
	void FireTimer() {
		auto callback = std::move(scheduled);
		scheduled = nullptr;
		callback();
	}

	//! Deliver the deferred response, standing in for a transport completing out of band
	void Complete() {
		auto status = failures_left > 0 ? failure_status : HTTPStatusCode::OK_200;
		if (failures_left > 0) {
			failures_left--;
		}
		auto response = make_uniq<HTTPResponse>(status);
		auto callback = std::move(pending);
		pending = nullptr;
		callback(std::move(response), ErrorData());
	}

	idx_t attempts = 0;
	idx_t waits = 0;
	uint64_t last_delay_ms = 0;
	HTTPResponseCallback pending;

private:
	//! 429 and 503 are throttle statuses and earn extra retries, anything else uses the plain ceiling
	HTTPStatusCode failure_status;
	idx_t failures_left;
	std::function<void()> scheduled;
};

class FlakyDeferringClient : public StubClient {
public:
	explicit FlakyDeferringClient(NonBlockingUtil &util) : util(util) {
	}

	HTTPRequestState Send(BaseRequest &request, HTTPExecutionMode mode, HTTPResponseCallback on_complete) override {
		util.attempts++;
		if (mode == HTTPExecutionMode::BLOCKING) {
			return HTTPClient::Send(request, mode, std::move(on_complete));
		}
		util.pending = std::move(on_complete);
		return HTTPRequestState::PENDING;
	}

private:
	NonBlockingUtil &util;
};

unique_ptr<HTTPClient> NonBlockingUtil::InitializeClient(HTTPParams &, const string &) {
	return make_uniq<FlakyDeferringClient>(*this);
}

} // namespace

TEST_CASE("HTTP core retries a request the transport deferred", "[api]") {
	NonBlockingUtil http_util(2, HTTPStatusCode::ServiceUnavailable_503);
	HTTPParams params(http_util);
	params.retries = 3;
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, params, nullptr, nullptr);
	unique_ptr<HTTPClient> client;

	idx_t completions = 0;
	bool succeeded = false;
	auto state = http_util.Send(request, client, HTTPExecutionMode::DEFERRABLE,
	                            [&](unique_ptr<HTTPResponse> response, ErrorData error) {
		                            completions++;
		                            succeeded = response && response->success;
	                            });

	// the first attempt is in flight, so nothing has been delivered yet
	REQUIRE(state == HTTPRequestState::PENDING);
	REQUIRE(http_util.attempts == 1);
	REQUIRE(completions == 0);

	// two 503s, each of which core turns into a scheduled retry rather than a failure
	http_util.Complete();
	REQUIRE(completions == 0);
	REQUIRE(http_util.waits == 1);
	http_util.FireTimer();
	REQUIRE(http_util.attempts == 2);

	http_util.Complete();
	REQUIRE(completions == 0);
	REQUIRE(http_util.waits == 2);
	http_util.FireTimer();
	REQUIRE(http_util.attempts == 3);

	// the third attempt succeeds, and only now is the caller told
	http_util.Complete();
	REQUIRE(completions == 1);
	REQUIRE(succeeded);
}

TEST_CASE("HTTP core gives up on a deferred request after its retries", "[api]") {
	// never succeeds, and a 500 is not a throttle status so the plain retry ceiling ends it
	NonBlockingUtil http_util(100, HTTPStatusCode::InternalServerError_500);
	HTTPParams params(http_util);
	params.retries = 1;
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, params, nullptr, nullptr);
	request.try_request = true;
	unique_ptr<HTTPClient> client;

	idx_t completions = 0;
	bool failed = false;
	http_util.Send(request, client, HTTPExecutionMode::DEFERRABLE,
	               [&](unique_ptr<HTTPResponse> response, ErrorData error) {
		               completions++;
		               failed = response && !response->success;
	               });

	http_util.Complete();
	http_util.FireTimer();
	http_util.Complete();

	REQUIRE(completions == 1);
	REQUIRE(failed);
	// one retry allowed, so two attempts total
	REQUIRE(http_util.attempts == 2);
}

TEST_CASE("HTTP default Wait sleeps rather than scheduling", "[api]") {
	HTTPUtil http_util;
	idx_t resumed = 0;
	http_util.Wait(0, [&]() { resumed++; });
	REQUIRE(resumed == 1);
}

TEST_CASE("HTTP core grants a throttled deferred request extra retries", "[api]") {
	// a 503 is a throttle status, so it earns THROTTLE_EXTRA_RETRIES on top of the configured ceiling
	NonBlockingUtil http_util(100, HTTPStatusCode::ServiceUnavailable_503);
	HTTPParams params(http_util);
	params.retries = 1;
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, params, nullptr, nullptr);
	request.try_request = true;
	unique_ptr<HTTPClient> client;

	idx_t completions = 0;
	http_util.Send(request, client, HTTPExecutionMode::DEFERRABLE,
	               [&](unique_ptr<HTTPResponse> response, ErrorData error) { completions++; });

	// drive it until core stops asking for retries
	for (idx_t i = 0; i < 20 && completions == 0; i++) {
		http_util.Complete();
		if (completions == 0) {
			http_util.FireTimer();
		}
	}
	REQUIRE(completions == 1);
	// 1 configured retry + 5 throttle retries + the original attempt
	REQUIRE(http_util.attempts == 7);
	REQUIRE(http_util.waits == 6);
	// and it actually backed off rather than hammering
	REQUIRE(http_util.last_delay_ms > 0);
}

TEST_CASE("HTTP retry policy never retries a non-idempotent request", "[api]") {
	RetryFixture fixture;
	fixture.params.retries = 3;
	HTTPHeaders headers;
	// POST is the one method that cannot be assumed safe to replay
	PostRequestInfo request("http://example.com/file", headers, fixture.params, nullptr, 0);
	HTTPRetryState state;
	uint64_t delay_ms = 0;

	// a 500 would be retried for a GET, but a POST has no retries at all
	auto attempt = ResponseAttempt(HTTPStatusCode::InternalServerError_500);
	REQUIRE(state.OnAttempt(request, attempt, delay_ms) == HTTPRetryDecision::FAILED);
}

TEST_CASE("HTTP retry policy still retries an idempotent request of the same shape", "[api]") {
	RetryFixture fixture;
	fixture.params.retries = 3;
	HTTPRetryState state;
	uint64_t delay_ms = 0;

	// the same status on a HEAD, which is idempotent, is retried
	auto attempt = ResponseAttempt(HTTPStatusCode::InternalServerError_500);
	REQUIRE(state.OnAttempt(fixture.request, attempt, delay_ms) == HTTPRetryDecision::RETRY);
}

namespace {

class ThreadedUtil;

//! A client whose transport completes on another thread, which is what makes the state a request
//! reports on the way out racy: the completion can land while Send is still unwinding.
//! It keeps itself alive across that completion, which is what a deferring backend has to do - the
//! request releases its own reference from inside the callback.
class ThreadedClient : public StubClient {
public:
	explicit ThreadedClient(ThreadedUtil &util) : util(util) {
	}

	HTTPRequestState Send(BaseRequest &request, HTTPExecutionMode mode, HTTPResponseCallback on_complete) override;

private:
	ThreadedUtil &util;
};

class ThreadedUtil : public HTTPUtil {
public:
	~ThreadedUtil() override {
		Join();
	}

	unique_ptr<HTTPClient> InitializeClient(HTTPParams &, const string &) override {
		return make_uniq<ThreadedClient>(*this);
	}

	void Join() {
		if (worker.joinable()) {
			worker.join();
		}
	}

	//! The thread belongs to the platform, not to the client, the way a real event loop does
	std::thread worker;
	//! Watches the client without holding it up, so the transport can check it outlived the callback
	weak_ptr<HTTPClient> client_watch;
	bool alive_after_completion = false;
};

HTTPRequestState ThreadedClient::Send(BaseRequest &request, HTTPExecutionMode mode, HTTPResponseCallback on_complete) {
	if (mode == HTTPExecutionMode::BLOCKING) {
		return HTTPClient::Send(request, mode, std::move(on_complete));
	}
	// the reference that keeps this client alive until the completion has returned
	auto self = shared_from_this();
	auto &platform = util;
	platform.client_watch = self;
	platform.worker = std::thread([self, &platform, callback = std::move(on_complete)]() mutable {
		auto response = make_uniq<HTTPResponse>(HTTPStatusCode::OK_200);
		response->success = true;
		callback(std::move(response), ErrorData());
		// done with the request, so let it go the way a transport does once it has delivered
		callback = nullptr;
		// that dropped the request's reference to us, and only [self] is still holding us up
		platform.alive_after_completion = !platform.client_watch.expired();
	});
	return HTTPRequestState::PENDING;
}

} // namespace

TEST_CASE("HTTP request completing on another thread is delivered exactly once", "[api]") {
	// repeated so the completion lands at varying points of Send's unwind
	for (idx_t run = 0; run < 200; run++) {
		ThreadedUtil http_util;
		HTTPParams params(http_util);
		HTTPHeaders headers;
		GetRequestInfo request("http://example.com/file", headers, params, nullptr, nullptr);
		unique_ptr<HTTPClient> client;

		atomic<idx_t> completions {0};
		auto state = http_util.Send(request, client, HTTPExecutionMode::DEFERRABLE,
		                            [&](unique_ptr<HTTPResponse> response, ErrorData error) { completions++; });
		http_util.Join();

		// whichever side won the race, the completion runs once and the state is one of the two answers
		REQUIRE(completions == 1);
		REQUIRE((state == HTTPRequestState::PENDING || state == HTTPRequestState::COMPLETED));
		// and the transport was not destroyed by the completion it delivered
		REQUIRE(http_util.alive_after_completion);
	}
}

namespace {

//! Stands in for a platform whose Wait schedules rather than sleeps, the Wasm case: it cannot block,
//! but it can still delay an attempt, so the backoff is worth granting
class SchedulingUtil : public HTTPUtil {
public:
	bool CanWait() const override {
		return true;
	}
};

//! A platform with no way to delay an attempt at all
class ImmediateUtil : public HTTPUtil {
public:
	bool CanWait() const override {
		return false;
	}
};

//! Drive a throttled request until the policy stops retrying, and report how many retries it granted
idx_t CountThrottledRetries(HTTPUtil &http_util) {
	HTTPParams params(http_util);
	params.retries = 1;
	HTTPHeaders headers;
	HeadRequestInfo request("http://example.com/file", headers, params);
	HTTPRetryState state;

	idx_t retries = 0;
	while (true) {
		auto attempt = ResponseAttempt(HTTPStatusCode::TooManyRequests_429);
		uint64_t delay_ms = 0;
		if (state.OnAttempt(request, attempt, delay_ms) != HTTPRetryDecision::RETRY) {
			return retries;
		}
		retries++;
	}
}

} // namespace

TEST_CASE("HTTP throttle retries follow the platform's ability to wait", "[api]") {
	// a platform that can delay gets the plain retry plus the throttle allowance
	SchedulingUtil scheduling;
	REQUIRE(CountThrottledRetries(scheduling) == 6);

	// one that cannot is left with the plain retry, since the extra ones would not back off
	ImmediateUtil immediate;
	REQUIRE(CountThrottledRetries(immediate) == 1);
}

TEST_CASE("HTTP timing of a deferred request spans the deferral", "[api]") {
	// succeeds on the first attempt, so the whole span is the one deferral the test controls
	NonBlockingUtil http_util(0, HTTPStatusCode::OK_200);
	HTTPParams params(http_util);
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, params, nullptr, nullptr);
	unique_ptr<HTTPClient> client;

	auto before = TimePoint::Tick();
	http_util.Send(request, client, HTTPExecutionMode::DEFERRABLE,
	               [](unique_ptr<HTTPResponse> response, ErrorData error) {});

	// the transport takes its time, which is exactly what the sync path could never measure
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	http_util.Complete();

	// the span opened at dispatch and closed at completion, so it contains the wait
	REQUIRE(TimePoint::ElapsedMillis(before, request.request_monotonic_start) >= 0);
	REQUIRE(TimePoint::ElapsedMillis(request.request_monotonic_start, request.request_monotonic_end) >= 10);
}

namespace {

//! Runs out of clients on the retry, which the synchronous path reports as a configuration error
class NullOnRetryUtil : public NonBlockingUtil {
public:
	NullOnRetryUtil() : NonBlockingUtil(1, HTTPStatusCode::InternalServerError_500) {
	}

	unique_ptr<HTTPClient> InitializeClient(HTTPParams &params, const string &proto_host_port) override {
		if (initialized++ > 0) {
			return nullptr;
		}
		return NonBlockingUtil::InitializeClient(params, proto_host_port);
	}

	idx_t initialized = 0;
};

} // namespace

TEST_CASE("HTTP deferred retry reports a null client instead of dereferencing it", "[api]") {
	NullOnRetryUtil http_util;
	HTTPParams params(http_util);
	params.retries = 3;
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, params, nullptr, nullptr);
	unique_ptr<HTTPClient> client;

	idx_t completions = 0;
	bool reported_error = false;
	http_util.Send(request, client, HTTPExecutionMode::DEFERRABLE,
	               [&](unique_ptr<HTTPResponse> response, ErrorData error) {
		               completions++;
		               reported_error = error.HasError();
	               });

	// the 500 asks for a retry, and the retry cannot get a client to make it with
	http_util.Complete();
	REQUIRE(completions == 1);
	REQUIRE(reported_error);
}

namespace {

//! A transport that completes on another thread and does not return from its own Send until that
//! completion is running. That is the window in which the caller must not be told COMPLETED.
class ConcurrentCompletionClient : public StubClient {
public:
	ConcurrentCompletionClient(std::thread &worker, atomic<bool> &entered) : worker(worker), entered(entered) {
	}

	HTTPRequestState Send(BaseRequest &, HTTPExecutionMode, HTTPResponseCallback on_complete) override {
		worker = std::thread([callback = std::move(on_complete)]() mutable {
			auto response = make_uniq<HTTPResponse>(HTTPStatusCode::OK_200);
			response->success = true;
			callback(std::move(response), ErrorData());
		});
		while (!entered) {
			std::this_thread::yield();
		}
		return HTTPRequestState::PENDING;
	}

private:
	std::thread &worker;
	atomic<bool> &entered;
};

} // namespace

TEST_CASE("HTTP completion is published only once the callback has run", "[api]") {
	HTTPUtil http_util;
	HTTPParams params(http_util);
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, params, nullptr, nullptr);

	std::thread worker;
	atomic<bool> entered {false};
	atomic<bool> release {false};
	atomic<bool> result_ready {false};
	unique_ptr<HTTPClient> client = make_uniq<ConcurrentCompletionClient>(worker, entered);

	idx_t completions = 0;
	auto state = http_util.Send(request, client, HTTPExecutionMode::DEFERRABLE,
	                            [&](unique_ptr<HTTPResponse> response, ErrorData error) {
		                            entered = true;
		                            while (!release) {
			                            std::this_thread::yield();
		                            }
		                            completions++;
		                            result_ready = true;
	                            });
	const bool ready_at_return = result_ready;
	release = true;
	worker.join();

	// the completion had not finished writing its result, so the caller must not have been told COMPLETED
	REQUIRE(!ready_at_return);
	REQUIRE(state == HTTPRequestState::PENDING);
	REQUIRE(completions == 1);
}

namespace {

//! A util whose transport hands every request off, so a test drives each attempt by hand
class SteeredUtil : public HTTPUtil {
public:
	unique_ptr<HTTPClient> InitializeClient(HTTPParams &http_params, const string &proto_host_port) override;

	unique_ptr<HTTPClient> InitializeClientExtended(HTTPParams &http_params, const string &proto_host_port,
	                                                const HTTPClientInitializationOptions &options) override {
		last_cache_policy = options.cache_policy;
		if (fail_init) {
			throw IOException("no client for you");
		}
		return InitializeClient(http_params, proto_host_port);
	}

	void Wait(uint64_t delay_ms, std::function<void()> resume) override {
		waits++;
		last_delay_ms = delay_ms;
		scheduled = std::move(resume);
	}

	//! Run whatever the retry asked us to come back to, standing in for a timer firing
	void FireTimer() {
		auto callback = std::move(scheduled);
		scheduled = nullptr;
		callback();
	}

	//! Deliver the outcome of the attempt currently in flight
	void Complete(unique_ptr<HTTPResponse> response, ErrorData error) {
		auto callback = std::move(pending);
		pending = nullptr;
		callback(std::move(response), std::move(error));
	}

	idx_t attempts = 0;
	idx_t waits = 0;
	uint64_t last_delay_ms = 0;
	bool fail_init = false;
	HTTPClientCachePolicy last_cache_policy = HTTPClientCachePolicy::DEFAULT;
	HTTPResponseCallback pending;

private:
	std::function<void()> scheduled;
};

class SteeredClient : public StubClient {
public:
	explicit SteeredClient(SteeredUtil &util) : util(util) {
	}

	HTTPRequestState Send(BaseRequest &, HTTPExecutionMode, HTTPResponseCallback on_complete) override {
		util.attempts++;
		util.pending = std::move(on_complete);
		return HTTPRequestState::PENDING;
	}

private:
	SteeredUtil &util;
};

unique_ptr<HTTPClient> SteeredUtil::InitializeClient(HTTPParams &, const string &) {
	return make_uniq<SteeredClient>(*this);
}

unique_ptr<HTTPResponse> StatusResponse(HTTPStatusCode status) {
	auto response = make_uniq<HTTPResponse>(status);
	response->success = false;
	return response;
}

//! The error a throttling server produces once a handler has turned its response into an exception
ErrorData ThrottleError() {
	HTTPResponse response(HTTPStatusCode::TooManyRequests_429);
	response.headers.Insert("Retry-After", "5");
	HTTPException throttled(response, "Request returned HTTP 429");
	return ErrorData(throttled);
}

} // namespace

TEST_CASE("HTTP deferred retry initialization failure completes the request", "[api]") {
	SteeredUtil http_util;
	HTTPParams params(http_util);
	params.retries = 1;
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, params, nullptr, nullptr);
	unique_ptr<HTTPClient> client = make_uniq<SteeredClient>(http_util);

	idx_t completions = 0;
	bool reported_error = false;
	http_util.Send(request, client, HTTPExecutionMode::DEFERRABLE,
	               [&](unique_ptr<HTTPResponse> response, ErrorData error) {
		               completions++;
		               reported_error = error.HasError();
	               });

	// the retry cannot get a client, and an accepted request still has to complete
	http_util.fail_init = true;
	http_util.Complete(StatusResponse(HTTPStatusCode::InternalServerError_500), ErrorData());
	REQUIRE(completions == 1);
	REQUIRE(reported_error);
	REQUIRE(http_util.waits == 0);
}

namespace {

//! A client with no asynchronous transport whose first attempt throws, as a dropped socket does
class ThrowOnceClient : public StubClient {
public:
	explicit ThrowOnceClient(idx_t &attempts) : attempts(attempts) {
	}

	unique_ptr<HTTPResponse> Get(GetRequestInfo &) override {
		attempts++;
		if (attempts == 1) {
			throw IOException("connection reset");
		}
		auto response = make_uniq<HTTPResponse>(HTTPStatusCode::OK_200);
		response->success = true;
		return response;
	}

private:
	idx_t &attempts;
};

class ThrowOnceUtil : public HTTPUtil {
public:
	unique_ptr<HTTPClient> InitializeClient(HTTPParams &, const string &) override {
		return make_uniq<ThrowOnceClient>(attempts);
	}

	idx_t attempts = 0;
};

} // namespace

TEST_CASE("HTTP deferred request retries a transport that throws", "[api]") {
	ThrowOnceUtil http_util;
	HTTPParams params(http_util);
	params.retries = 1;
	params.retry_wait_ms = 0;
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, params, nullptr, nullptr);

	idx_t completions = 0;
	bool succeeded = false;
	unique_ptr<HTTPClient> client = make_uniq<ThrowOnceClient>(http_util.attempts);
	auto state = http_util.Send(request, client, HTTPExecutionMode::DEFERRABLE,
	                            [&](unique_ptr<HTTPResponse> response, ErrorData error) {
		                            completions++;
		                            succeeded = response && response->success;
	                            });

	// the throw is an attempt like any other, so the retry that the blocking path would make happens here too
	REQUIRE(state == HTTPRequestState::COMPLETED);
	REQUIRE(completions == 1);
	REQUIRE(succeeded);
	REQUIRE(http_util.attempts == 2);
}

TEST_CASE("HTTP deferred error keeps its status, Retry-After and type", "[api]") {
	SteeredUtil http_util;
	HTTPParams params(http_util);
	params.retries = 1;
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, params, nullptr, nullptr);
	unique_ptr<HTTPClient> client = make_uniq<SteeredClient>(http_util);

	idx_t completions = 0;
	ExceptionType reported_type = ExceptionType::INVALID;
	http_util.Send(request, client, HTTPExecutionMode::DEFERRABLE,
	               [&](unique_ptr<HTTPResponse> response, ErrorData error) {
		               completions++;
		               reported_type = error.Type();
	               });

	// a 429 is a throttle status even when a handler folded it into an exception, so it earns the extra
	// retries and the Retry-After the response carried
	http_util.Complete(nullptr, ThrottleError());
	REQUIRE(http_util.waits == 1);
	// Retry-After is 5s, and the jitter only ever subtracts half of it
	REQUIRE(http_util.last_delay_ms >= 2500);
	REQUIRE(http_util.last_delay_ms <= 5000);

	// one configured retry plus five throttle retries, so seven attempts in total
	while (http_util.attempts < 7) {
		http_util.FireTimer();
		http_util.Complete(nullptr, ThrottleError());
	}
	REQUIRE(completions == 1);
	REQUIRE(reported_type == ExceptionType::HTTP);
}

TEST_CASE("HTTP deferred retry bypasses the cache after a transport failure", "[api]") {
	SteeredUtil http_util;
	HTTPParams params(http_util);
	params.retries = 2;
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, params, nullptr, nullptr);
	unique_ptr<HTTPClient> client = make_uniq<SteeredClient>(http_util);

	http_util.Send(request, client, HTTPExecutionMode::DEFERRABLE, [](unique_ptr<HTTPResponse>, ErrorData) {});

	// an attempt that produced no response failed in the transport, which must not be reused
	http_util.Complete(nullptr, ErrorData(IOException("connection reset")));
	REQUIRE(http_util.last_cache_policy == HTTPClientCachePolicy::BYPASS_CACHE);

	// a server that answered leaves the transport intact, exactly as the synchronous path decides
	http_util.last_cache_policy = HTTPClientCachePolicy::BYPASS_CACHE;
	http_util.FireTimer();
	http_util.Complete(StatusResponse(HTTPStatusCode::InternalServerError_500), ErrorData());
	REQUIRE(http_util.last_cache_policy == HTTPClientCachePolicy::DEFAULT);
}

TEST_CASE("HTTP completion that throws is not invoked a second time", "[api]") {
	SteeredUtil http_util;
	HTTPParams params(http_util);
	params.retries = 0;
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, params, nullptr, nullptr);
	request.try_request = true;
	unique_ptr<HTTPClient> client = make_uniq<SteeredClient>(http_util);

	idx_t completions = 0;
	http_util.Send(request, client, HTTPExecutionMode::DEFERRABLE,
	               [&](unique_ptr<HTTPResponse> response, ErrorData error) {
		               completions++;
		               throw IOException("the caller could not take the result");
	               });

	// the throw belongs to the caller, and it does not earn them a second completion
	REQUIRE_THROWS(http_util.Complete(StatusResponse(HTTPStatusCode::InternalServerError_500), ErrorData()));
	REQUIRE(completions == 1);
}

TEST_CASE("HTTP deferred response carries the request url", "[api]") {
	SteeredUtil http_util;
	HTTPParams params(http_util);
	HTTPHeaders headers;
	GetRequestInfo request("http://example.com/file", headers, params, nullptr, nullptr);
	unique_ptr<HTTPClient> client = make_uniq<SteeredClient>(http_util);

	string reported_url;
	http_util.Send(request, client, HTTPExecutionMode::DEFERRABLE,
	               [&](unique_ptr<HTTPResponse> response, ErrorData error) {
		               reported_url = response ? response->url : string();
	               });

	auto response = make_uniq<HTTPResponse>(HTTPStatusCode::OK_200);
	response->success = true;
	http_util.Complete(std::move(response), ErrorData());
	REQUIRE(reported_url == request.url);
}
