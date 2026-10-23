//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/caller_participation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! Whether the thread that submits a query is known to drive it to completion right after submission
enum class CallerParticipation : uint8_t {
	//! The caller may return before the query completes and pull tasks later, from any thread, or not at all
	MAY_DETACH,
	//! The submitting call itself executes tasks until the query completes, so one task is left to it
	DRIVES_TO_COMPLETION
};

} // namespace duckdb
