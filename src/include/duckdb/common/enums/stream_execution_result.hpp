//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/stream_execution_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class StreamExecutionResult : uint8_t {
	CHUNK_READY,
	CHUNK_NOT_READY,
	EXECUTION_ERROR,
	EXECUTION_CANCELLED,
	BLOCKED,
	NO_TASKS_AVAILABLE,
	EXECUTION_FINISHED
};

} // namespace duckdb
