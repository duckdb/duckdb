//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/task_scheduler_pool_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class TaskSchedulerPoolType : uint8_t { REGULAR, ASYNC, ENUM_SIZE };

static constexpr size_t TASK_SCHEDULER_POOL_TYPE_COUNT = static_cast<size_t>(TaskSchedulerPoolType::ENUM_SIZE);

} // namespace duckdb
