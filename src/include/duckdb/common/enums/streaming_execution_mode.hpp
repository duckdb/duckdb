//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/streaming_execution_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! SYNC is the default. Fetch calls drive execution and restart blocked producers.
//! In ASYNC mode background workers keep the result buffer filled. Fetching never
//! runs execution. Selected via the global streaming_execution_mode setting. Applies
//! only to streaming results. Read once per query at submission.
enum class StreamingExecutionMode : uint8_t { SYNC, ASYNC };

} // namespace duckdb
