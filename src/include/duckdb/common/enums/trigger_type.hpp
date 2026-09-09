//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/trigger_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class TriggerTiming : uint8_t { BEFORE = 0, AFTER = 1, INSTEAD_OF = 2 };

enum class TriggerEventType : uint8_t { INSERT_EVENT = 0, DELETE_EVENT = 1, UPDATE_EVENT = 2 };

enum class TriggerForEach : uint8_t { STATEMENT = 0, ROW = 1 };

} // namespace duckdb
