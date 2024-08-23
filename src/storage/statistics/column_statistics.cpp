#include "duckdb/storage/statistics/column_statistics.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

ColumnStatistics::ColumnStatistics(BaseStatistics stats_p) : stats(std::move(stats_p)) {
	if (DistinctStatistics::TypeIsSupported(stats.GetType())) {
		distinct_stats = make_uniq<DistinctStatistics>();
	}
}
ColumnStatistics::ColumnStatistics(BaseStatistics stats_p, unique_ptr<DistinctStatistics> distinct_stats_p)
    : stats(std::move(stats_p)), distinct_stats(std::move(distinct_stats_p)) {
}

shared_ptr<ColumnStatistics> ColumnStatistics::CreateEmptyStats(const LogicalType &type) {
	return make_shared_ptr<ColumnStatistics>(BaseStatistics::CreateEmpty(type));
}

void ColumnStatistics::Merge(ColumnStatistics &other) {
	stats.Merge(other.stats);
	if (distinct_stats) {
		D_ASSERT(other.distinct_stats);
		distinct_stats->Merge(*other.distinct_stats);
	}
}

BaseStatistics &ColumnStatistics::Statistics() {
	return stats;
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

void ColumnStatistics::SetDistinct(unique_ptr<DistinctStatistics> distinct) {
	this->distinct_stats = std::move(distinct);
}

void ColumnStatistics::UpdateDistinctStatistics(Vector &v, idx_t count) {
	static constexpr idx_t MAXIMUM_STRING_LENGTH_FOR_DISTINCT = 64;
	if (!distinct_stats) {
		return;
	}
	if (stats.GetType().InternalType() == PhysicalType::VARCHAR && StringStats::HasMaxStringLength(stats) &&
	    StringStats::MaxStringLength(stats) > MAXIMUM_STRING_LENGTH_FOR_DISTINCT) {
		// We start bailing out on distinct statistics if we encounter long strings,
		// because hashing them for HLL is expensive and they probably won't be used as join keys anyway.
		// If they are used as join keys, we will still have decent join orders (same method as Parquet)
		distinct_stats.reset();
		return;
	}
	distinct_stats->Update(v, count);
}

shared_ptr<ColumnStatistics> ColumnStatistics::Copy() const {
	return make_shared_ptr<ColumnStatistics>(stats.Copy(), distinct_stats ? distinct_stats->Copy() : nullptr);
}

void ColumnStatistics::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "statistics", stats);
	serializer.WritePropertyWithDefault(101, "distinct", distinct_stats, unique_ptr<DistinctStatistics>());
}

shared_ptr<ColumnStatistics> ColumnStatistics::Deserialize(Deserializer &deserializer) {
	auto stats = deserializer.ReadProperty<BaseStatistics>(100, "statistics");
	auto distinct_stats = deserializer.ReadPropertyWithExplicitDefault<unique_ptr<DistinctStatistics>>(
	    101, "distinct", unique_ptr<DistinctStatistics>());
	return make_shared_ptr<ColumnStatistics>(std::move(stats), std::move(distinct_stats));
}

} // namespace duckdb
