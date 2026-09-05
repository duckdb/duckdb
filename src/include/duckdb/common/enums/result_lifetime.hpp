//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/result_lifetime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! How long a result sink keeps what producers append. Left open by a streaming request and
//! settled by the consumer's first call: a fetch drains, a materialize retains
enum class ResultLifetime : uint8_t {
	//! Not chosen yet: producers park their first chunk unconsumed until the consumer chooses
	UNDECIDED,
	//! Bounded buffer for single-consume streaming results
	DRAINING,
	//! Unbounded buffer for a retained / materialized result
	RETAINED
};

} // namespace duckdb
