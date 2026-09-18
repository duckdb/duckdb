//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/task_scheduler_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class TaskSchedulerType : uint8_t { REGULAR, ASYNC, ENUM_SIZE };

static constexpr size_t TASK_SCHEDULER_TYPE_COUNT = static_cast<size_t>(TaskSchedulerType::ENUM_SIZE);

} // namespace duckdb
