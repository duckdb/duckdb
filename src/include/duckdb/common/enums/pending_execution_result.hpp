//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/pending_execution_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class PendingExecutionResult : uint8_t {
	RESULT_READY,
	RESULT_NOT_READY,
	EXECUTION_ERROR,
	BLOCKED,
	NO_TASKS_AVAILABLE,
	EXECUTION_FINISHED
};

} // namespace duckdb
