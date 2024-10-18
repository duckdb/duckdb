#include "duckdb/storage/table/table_statistics.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/execution/reservoir_sample.hpp"

namespace duckdb {

void TableStatistics::Initialize(const vector<LogicalType> &types, PersistentTableData &data) {
	D_ASSERT(Empty());

	stats_lock = make_shared_ptr<mutex>();
	column_stats = std::move(data.table_stats.column_stats);
	if (column_stats.size() != types.size()) { // LCOV_EXCL_START
		throw IOException("Table statistics column count is not aligned with table column count. Corrupt file?");
	} // LCOV_EXCL_STOP
}

void TableStatistics::InitializeEmpty(const vector<LogicalType> &types) {
	D_ASSERT(Empty());

	stats_lock = make_shared_ptr<mutex>();
	for (auto &type : types) {
		column_stats.push_back(ColumnStatistics::CreateEmptyStats(type));
	}
}

void TableStatistics::InitializeAddColumn(TableStatistics &parent, const LogicalType &new_column_type) {
	D_ASSERT(Empty());
	D_ASSERT(parent.stats_lock);

	stats_lock = parent.stats_lock;
	lock_guard<mutex> lock(*stats_lock);
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		column_stats.push_back(parent.column_stats[i]);
	}
	column_stats.push_back(ColumnStatistics::CreateEmptyStats(new_column_type));
}

void TableStatistics::InitializeRemoveColumn(TableStatistics &parent, idx_t removed_column) {
	D_ASSERT(Empty());
	D_ASSERT(parent.stats_lock);

	stats_lock = parent.stats_lock;
	lock_guard<mutex> lock(*stats_lock);
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		if (i != removed_column) {
			column_stats.push_back(parent.column_stats[i]);
		}
	}
}

void TableStatistics::InitializeAlterType(TableStatistics &parent, idx_t changed_idx, const LogicalType &new_type) {
	D_ASSERT(Empty());
	D_ASSERT(parent.stats_lock);

	stats_lock = parent.stats_lock;
	lock_guard<mutex> lock(*stats_lock);
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		if (i == changed_idx) {
			column_stats.push_back(ColumnStatistics::CreateEmptyStats(new_type));
		} else {
			column_stats.push_back(parent.column_stats[i]);
		}
	}
}

void TableStatistics::InitializeAddConstraint(TableStatistics &parent) {
	D_ASSERT(Empty());
	D_ASSERT(parent.stats_lock);

	stats_lock = parent.stats_lock;
	lock_guard<mutex> lock(*stats_lock);
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		column_stats.push_back(parent.column_stats[i]);
	}
}

void TableStatistics::MergeStats(TableStatistics &other) {
	auto l = GetLock();
	D_ASSERT(column_stats.size() == other.column_stats.size());
	for (idx_t i = 0; i < column_stats.size(); i++) {
		if (column_stats[i]) {
			D_ASSERT(other.column_stats[i]);
			column_stats[i]->Merge(*other.column_stats[i]);
		}
	}
}

void TableStatistics::MergeStats(idx_t i, BaseStatistics &stats) {
	auto l = GetLock();
	MergeStats(*l, i, stats);
}

void TableStatistics::MergeStats(TableStatisticsLock &lock, idx_t i, BaseStatistics &stats) {
	column_stats[i]->Statistics().Merge(stats);
}

ColumnStatistics &TableStatistics::GetStats(TableStatisticsLock &lock, idx_t i) {
	return *column_stats[i];
}

unique_ptr<BaseStatistics> TableStatistics::CopyStats(idx_t i) {
	lock_guard<mutex> l(*stats_lock);
	auto result = column_stats[i]->Statistics().Copy();
	if (column_stats[i]->HasDistinctStats()) {
		result.SetDistinctCount(column_stats[i]->DistinctStats().GetCount());
	}
	return result.ToUnique();
}

void TableStatistics::CopyStats(TableStatistics &other) {
	TableStatisticsLock lock(*stats_lock);
	CopyStats(lock, other);
}

void TableStatistics::CopyStats(TableStatisticsLock &lock, TableStatistics &other) {
	D_ASSERT(other.Empty());
	other.stats_lock = make_shared_ptr<mutex>();
	for (auto &stats : column_stats) {
		other.column_stats.push_back(stats->Copy());
	}
}

void TableStatistics::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "column_stats", column_stats);
	serializer.WritePropertyWithDefault<unique_ptr<BlockingSample>>(101, "table_sample", table_sample, nullptr);
}

void TableStatistics::Deserialize(Deserializer &deserializer, ColumnList &columns) {
	auto physical_columns = columns.Physical();

	auto iter = physical_columns.begin();
	deserializer.ReadList(100, "column_stats", [&](Deserializer::List &list, idx_t i) {
		auto &col = *iter;
		iter.operator++();

		auto type = col.GetType();
		deserializer.Set<LogicalType &>(type);

		column_stats.push_back(list.ReadElement<shared_ptr<ColumnStatistics>>());

		deserializer.Unset<LogicalType>();
	});
	table_sample =
	    deserializer.ReadPropertyWithExplicitDefault<unique_ptr<BlockingSample>>(101, "table_sample", nullptr);
}

unique_ptr<TableStatisticsLock> TableStatistics::GetLock() {
	D_ASSERT(stats_lock);
	return make_uniq<TableStatisticsLock>(*stats_lock);
}

bool TableStatistics::Empty() {
	D_ASSERT(column_stats.empty() == (stats_lock.get() == nullptr));
	return column_stats.empty();
}

} // namespace duckdb
