//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/cast/cast_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {

struct CastStatistics {
	static bool CanPropagate(const LogicalType &source, const LogicalType &target);
	static unique_ptr<BaseStatistics> TryPropagate(const BaseStatistics &stats, const LogicalType &source,
	                                               const LogicalType &target);
	static unique_ptr<BaseStatistics> Propagate(CastStatisticsInput &input);
};

} // namespace duckdb
