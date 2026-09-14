//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/debug_sql_export_verification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class DebugSQLExportVerification : uint8_t { OFF, REPORT, STRICT };

} // namespace duckdb
