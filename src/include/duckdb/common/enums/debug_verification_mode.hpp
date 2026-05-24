//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/debug_verification_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class DebugVerificationMode : uint8_t { DEFAULT, NONE, VERIFY_VECTORS, VERIFY_SERIALIZATION };

} // namespace duckdb
