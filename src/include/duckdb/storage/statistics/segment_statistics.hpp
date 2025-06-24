//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/segment_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

class SegmentStatistics {
public:
	explicit SegmentStatistics(LogicalType type);
	explicit SegmentStatistics(BaseStatistics statistics);

	//! Type-specific statistics of the segment
	BaseStatistics statistics;
};

} // namespace duckdb
