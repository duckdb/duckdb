#include "duckdb/storage/statistics/column_statistics.hpp"

namespace duckdb {

ColumnStatistics::ColumnStatistics(unique_ptr<BaseStatistics> stats_p) : stats(move(stats_p)) {
}

shared_ptr<ColumnStatistics> ColumnStatistics::CreateEmptyStats(const LogicalType &type) {
	auto col_stats = BaseStatistics::CreateEmpty(type, StatisticsType::GLOBAL_STATS);
	return make_shared<ColumnStatistics>(move(col_stats));
}

} // namespace duckdb
