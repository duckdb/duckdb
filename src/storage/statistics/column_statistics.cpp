#include "duckdb/storage/statistics/column_statistics.hpp"

namespace duckdb {

ColumnStatistics::ColumnStatistics(unique_ptr<BaseStatistics> stats_p) : stats(std::move(stats_p)) {
	auto type = stats->GetType().InternalType();
	if (type != PhysicalType::LIST && type != PhysicalType::STRUCT) {
		distinct_stats = make_unique<DistinctStatistics>();
	}
}

shared_ptr<ColumnStatistics> ColumnStatistics::CreateEmptyStats(const LogicalType &type) {
	auto col_stats = BaseStatistics::CreateEmpty(type);
	return make_shared<ColumnStatistics>(std::move(col_stats));
}

void ColumnStatistics::Merge(ColumnStatistics &other) {
	stats->Merge(*other.stats);
	if (distinct_stats) {
		distinct_stats->Merge(*other.distinct_stats);
	}
}

BaseStatistics &ColumnStatistics::Statistics() {
	if (!stats) {
		throw InternalException("Statistics called without stats");
	}
	return *stats;
}

bool ColumnStatistics::HasDistinctStats() {
	return distinct_stats.get();
}

DistinctStatistics &ColumnStatistics::DistinctStats() {
	if (!distinct_stats) {
		throw InternalException("DistinctStats called without distinct_stats");
	}
	return *distinct_stats;
}

void ColumnStatistics::SetDistinct(unique_ptr<DistinctStatistics> distinct_stats) {
	this->distinct_stats = std::move(distinct_stats);
}

void ColumnStatistics::UpdateDistinctStatistics(Vector &v, idx_t count) {
	if (!distinct_stats) {
		return;
	}
	auto &d_stats = (DistinctStatistics &)*distinct_stats;
	d_stats.Update(v, count);
}

} // namespace duckdb
