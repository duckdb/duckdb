//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/read_policy_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class ReadPolicyType : uint8_t { DEFAULT = 0, ALIGNED = 1 };

} // namespace duckdb
