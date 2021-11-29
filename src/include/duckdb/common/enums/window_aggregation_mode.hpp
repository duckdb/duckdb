//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/window_aggregation_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class WindowAggregationMode : uint32_t {
	//! Use the window aggregate API if available
	WINDOW = 0,
	//! Don't use window, but use combine if available
	COMBINE,
	//! Don't use combine or window (compute each frame separately)
	SEPARATE
};

} // namespace duckdb
