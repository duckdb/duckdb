//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/column_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

class ColumnStatistics {
public:
	explicit ColumnStatistics(unique_ptr<BaseStatistics> stats_p);

	unique_ptr<BaseStatistics> stats;

public:
	static shared_ptr<ColumnStatistics> CreateEmptyStats(const LogicalType &type);
};

} // namespace duckdb
