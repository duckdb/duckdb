//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/http/http_retry_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct HTTPParams;

//! Operation-local retry budget shared by successive requests, not concurrent requests.
class HTTPRetryState {
	friend class HTTPUtil;

public:
	DUCKDB_API explicit HTTPRetryState(const HTTPParams &params);
	HTTPRetryState(const HTTPRetryState &) = delete;
	HTTPRetryState &operator=(const HTTPRetryState &) = delete;

public:
	//! Caller must establish replay safety first; this only admits a retry, it does not send one.
	//! True consumes one retry and waits; false neither consumes a retry nor waits.
	[[nodiscard]] DUCKDB_API bool TryRetry();

private:
	enum class RetryType : uint8_t { NORMAL, THROTTLED };
	//! Only core's throttle classification may grant the extra allowance, for this retry only.
	[[nodiscard]] bool TryRetry(RetryType type, const string &retry_after);

private:
	//! Retry settings captured for the operation.
	const uint64_t retries;
	const uint64_t retry_wait_ms;
	const float retry_backoff;

	//! Retries admitted across all participating requests.
	uint64_t retries_used = 0;
};

} // namespace duckdb
