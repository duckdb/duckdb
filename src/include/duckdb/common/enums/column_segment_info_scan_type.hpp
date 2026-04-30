//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/column_segment_info_scan_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class ColumnSegmentInfoScanType : uint8_t { ALL = 0, ONLY_LOADED_SEGMENTS = 1 };

ColumnSegmentInfoScanType ColumnSegmentInfoScanTypeFromString(const string &input);

} // namespace duckdb
