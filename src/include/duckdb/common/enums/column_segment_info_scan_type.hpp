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

//! Options controlling how pragma_storage_info gathers column segment info.
//! Both axes are independent:
//!   * include_segment_info: when true, populate the per-segment `segment_info` column
//!     (this requires reading per-segment data — expensive for bitpacked segments).
//!   * loaded_segments_only: when true, skip columns whose metadata has not been
//!     lazy-loaded; used to introspect what is resident without triggering loads.
struct ColumnSegmentInfoScanOptions {
	bool include_segment_info = false;
	bool loaded_segments_only = false;
};

} // namespace duckdb
