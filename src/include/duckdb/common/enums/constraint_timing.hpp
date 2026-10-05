//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/constraint_timing.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! When a constraint is checked: eagerly per row (default), at the end of a statement, or at commit
enum class ConstraintTiming : uint8_t { EAGER, IMMEDIATE, DEFERRED };

} // namespace duckdb
