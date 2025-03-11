//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/scan_vector_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class ScanVectorType { SCAN_ENTIRE_VECTOR, SCAN_FLAT_VECTOR };

enum class ScanVectorMode { REGULAR_SCAN, SCAN_COMMITTED, SCAN_COMMITTED_NO_UPDATES };

} // namespace duckdb
