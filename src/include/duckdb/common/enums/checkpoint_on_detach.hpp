//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/checkpoint_on_detach.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class CheckpointOnDetach : uint8_t { DEFAULT = 0, ENABLED = 1, DISABLED = 2 };

} // namespace duckdb
