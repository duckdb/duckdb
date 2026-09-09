#include "duckdb/main/http/http_retry_budget.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/main/http/http_util.hpp"

#ifndef DUCKDB_NO_THREADS
#include <chrono>
#include <cmath>
#include <thread>
#endif

namespace duckdb {

HTTPRetryDecision HTTPRetryDecision::Finish() {
	return HTTPRetryDecision(Type::FINISH);
}

HTTPRetryDecision HTTPRetryDecision::Retry() {
	return HTTPRetryDecision(Type::RETRY);
}

HTTPRetryDecision HTTPRetryDecision::Throttled(const string &retry_after) {
	return HTTPRetryDecision(Type::THROTTLED, retry_after);
}

HTTPRetryBudget::HTTPRetryBudget(const HTTPParams &params)
    : retries(params.retries), retry_wait_ms(params.retry_wait_ms), retry_backoff(params.retry_backoff) {
}

void HTTPRetryBudget::Run(const std::function<HTTPRetryDecision()> &attempt) {
	Run(attempt, {});
}

void HTTPRetryBudget::Run(const std::function<HTTPRetryDecision()> &attempt,
                          const std::function<void()> &before_retry) {
	if (!attempt) {
		throw InternalException("HTTP retry loop requires an attempt callback");
	}
	for (;;) {
		auto decision = attempt();
		if (decision.type == HTTPRetryDecision::Type::FINISH || !ConsumeAndWait(decision)) {
			return;
		}
		if (before_retry) {
			before_retry();
		}
	}
}

bool HTTPRetryBudget::ConsumeAndWait(const HTTPRetryDecision &decision) {
	D_ASSERT(decision.type != HTTPRetryDecision::Type::FINISH);
	const bool throttled = decision.type == HTTPRetryDecision::Type::THROTTLED;
#ifndef DUCKDB_NO_THREADS
	static constexpr uint64_t THROTTLE_EXTRA_RETRIES = 5;
#else
	// Without threads we cannot sleep between retries, so do not add zero-delay retries.
	static constexpr uint64_t THROTTLE_EXTRA_RETRIES = 0;
#endif
	const auto extra_retries = throttled ? THROTTLE_EXTRA_RETRIES : 0;
	if (retries_used >= retries && retries_used - retries >= extra_retries) {
		return false;
	}
	retries_used++;
#ifndef DUCKDB_NO_THREADS
	if (retries_used > 1 || throttled) {
		static constexpr uint64_t THROTTLE_MAX_BACKOFF_MS = 10000;
		const auto backoff_exp = static_cast<double>(throttled ? retries_used - 1 : retries_used - 2);
		const auto backoff_ms = (double)retry_wait_ms * pow(retry_backoff, backoff_exp);
		// Cap in the double domain to avoid overflow in the cast.
		uint64_t sleep_amount = (uint64_t)MinValue<double>(backoff_ms, (double)NumericLimits<int64_t>::Maximum());
		if (throttled) {
			sleep_amount = MinValue<uint64_t>(sleep_amount, THROTTLE_MAX_BACKOFF_MS);
			if (!decision.retry_after.empty()) {
				// Honor a numeric Retry-After (seconds), capped like the backoff.
				uint64_t retry_after_s = 0;
				if (TryCast::Operation<string_t, uint64_t>(string_t(decision.retry_after), retry_after_s)) {
					retry_after_s = MinValue<uint64_t>(retry_after_s, THROTTLE_MAX_BACKOFF_MS / 1000);
					sleep_amount = MaxValue<uint64_t>(sleep_amount, retry_after_s * 1000);
				}
			}
			// Subtractive jitter ([base/2, base]) de-synchronizes retry bursts while honoring the cap.
			RandomEngine random;
			sleep_amount -= random.NextRandomInteger64() % (sleep_amount / 2 + 1);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(sleep_amount));
	}
#else
	(void)retry_wait_ms;
	(void)retry_backoff;
#endif
	return true;
}

} // namespace duckdb
