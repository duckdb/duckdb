#include "duckdb/storage/table/table_statistics.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"

namespace duckdb {

void TableStatistics::Initialize(const vector<LogicalType> &types, PersistentTableData &data) {
	D_ASSERT(Empty());

	column_stats = move(data.column_stats);
	if (column_stats.size() != types.size()) { // LCOV_EXCL_START
		throw IOException("Table statistics column count is not aligned with table column count. Corrupt file?");
	} // LCOV_EXCL_STOP
}

void TableStatistics::InitializeEmpty(const vector<LogicalType> &types) {
	D_ASSERT(Empty());

	for (auto &type : types) {
		column_stats.push_back(BaseStatistics::CreateEmpty(type));
	}
}

void TableStatistics::InitializeAddColumn(TableStatistics &parent, const LogicalType &new_column_type) {
	D_ASSERT(Empty());

	lock_guard<mutex> stats_lock(parent.stats_lock);
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		column_stats.push_back(parent.column_stats[i]->Copy());
	}
	column_stats.push_back(BaseStatistics::CreateEmpty(new_column_type));
}

void TableStatistics::InitializeRemoveColumn(TableStatistics &parent, idx_t removed_column) {
	D_ASSERT(Empty());

	lock_guard<mutex> stats_lock(parent.stats_lock);
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		if (i != removed_column) {
			column_stats.push_back(parent.column_stats[i]->Copy());
		}
	}
}

void TableStatistics::InitializeAlterType(TableStatistics &parent, idx_t changed_idx, const LogicalType &new_type) {
	D_ASSERT(Empty());

	lock_guard<mutex> stats_lock(parent.stats_lock);
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		if (i == changed_idx) {
			column_stats.push_back(BaseStatistics::CreateEmpty(new_type));
		} else {
			column_stats.push_back(parent.column_stats[i]->Copy());
		}
	}
}

void TableStatistics::MergeStats(idx_t i, BaseStatistics &stats) {
	auto l = GetLock();
	MergeStats(*l, i, stats);
}

void TableStatistics::MergeStats(TableStatisticsLock &lock, idx_t i, BaseStatistics &stats) {
	column_stats[i]->Merge(stats);
}

BaseStatistics &TableStatistics::GetStats(idx_t i) {
	return *column_stats[i];
}

unique_ptr<BaseStatistics> TableStatistics::CopyStats(idx_t i) {
	lock_guard<mutex> l(stats_lock);
	return column_stats[i]->Copy();
}

unique_ptr<TableStatisticsLock> TableStatistics::GetLock() {
	return make_unique<TableStatisticsLock>(stats_lock);
}

bool TableStatistics::Empty() {
	return column_stats.empty();
}

} // namespace duckdb
