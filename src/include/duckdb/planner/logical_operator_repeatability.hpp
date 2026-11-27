#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class LogicalOperator;

enum class LogicalOperatorRepeatability : uint8_t { REPEATABLE, NON_REPEATABLE, UNKNOWN };

//! Classifies this operator's type and expressions, without inspecting its children.
LogicalOperatorRepeatability ClassifyLogicalOperatorLocalRepeatability(LogicalOperator &op);
//! Also rejects operators whose descendants have side effects.
LogicalOperatorRepeatability ClassifyLogicalOperatorRepeatability(LogicalOperator &op);
bool LogicalSubtreeIsRepeatable(LogicalOperator &op);

} // namespace duckdb
