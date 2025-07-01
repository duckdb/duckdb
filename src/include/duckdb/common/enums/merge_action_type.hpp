//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/merge_action_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class MergeActionType : uint8_t { MERGE_UPDATE, MERGE_DELETE, MERGE_INSERT, MERGE_DO_NOTHING, MERGE_ERROR };

} // namespace duckdb
