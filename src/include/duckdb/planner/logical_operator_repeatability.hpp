#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class LogicalOperator;

enum class LogicalOperatorRepeatability : uint8_t { REPEATABLE, NON_REPEATABLE, UNKNOWN };

LogicalOperatorRepeatability ClassifyLogicalOperatorRepeatability(LogicalOperator &op);
bool LogicalSubtreeIsRepeatable(LogicalOperator &op);

} // namespace duckdb
