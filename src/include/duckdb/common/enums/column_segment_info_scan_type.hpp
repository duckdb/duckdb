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

//! What pragma_storage_info should report.
//! - STANDARD: skip the (expensive) per-segment `segment_info` column.
//! - EXTENDED: include the `segment_info` column.
//! - EXTENDED_ONLY_LOADED: like EXTENDED, but skip columns whose metadata has not been lazy-loaded.
enum class ColumnSegmentInfoScanType : uint8_t { STANDARD = 0, EXTENDED = 1, EXTENDED_ONLY_LOADED = 2 };

//! Whether the scan type populates the per-segment `segment_info` column.
inline bool ColumnSegmentInfoIncludesSegmentInfo(ColumnSegmentInfoScanType t) {
	return t == ColumnSegmentInfoScanType::EXTENDED || t == ColumnSegmentInfoScanType::EXTENDED_ONLY_LOADED;
}

//! Whether the scan type should skip columns that have not been lazy-loaded.
inline bool ColumnSegmentInfoOnlyLoaded(ColumnSegmentInfoScanType t) {
	return t == ColumnSegmentInfoScanType::EXTENDED_ONLY_LOADED;
}

ColumnSegmentInfoScanType ColumnSegmentInfoScanTypeFromString(const string &input);

} // namespace duckdb
