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
	SegmentStatistics(LogicalType type);
	SegmentStatistics(LogicalType type, unique_ptr<BaseStatistics> statistics);

	LogicalType type;

	//! Type-specific statistics of the segment
	unique_ptr<BaseStatistics> statistics;

public:
	void Reset();
};

} // namespace duckdb
