#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

void StructStats::Construct(BaseStatistics &stats) {
	auto &child_types = StructType::GetChildTypes(stats.GetType());
	stats.child_stats = unsafe_unique_array<BaseStatistics>(new BaseStatistics[child_types.size()]);
	for (idx_t i = 0; i < child_types.size(); i++) {
		BaseStatistics::Construct(stats.child_stats[i], child_types[i].second);
	}
}

BaseStatistics StructStats::CreateUnknown(LogicalType type) {
	auto &child_types = StructType::GetChildTypes(type);
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	for (idx_t i = 0; i < child_types.size(); i++) {
		result.child_stats[i].Copy(BaseStatistics::CreateUnknown(child_types[i].second));
	}
	return result;
}

BaseStatistics StructStats::CreateEmpty(LogicalType type) {
	auto &child_types = StructType::GetChildTypes(type);
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	for (idx_t i = 0; i < child_types.size(); i++) {
		result.child_stats[i].Copy(BaseStatistics::CreateEmpty(child_types[i].second));
	}
	return result;
}

const BaseStatistics *StructStats::GetChildStats(const BaseStatistics &stats) {
	if (stats.GetStatsType() != StatisticsType::STRUCT_STATS) {
		throw InternalException("Calling StructStats::GetChildStats on stats that is not a struct");
	}
	return stats.child_stats.get();
}

const BaseStatistics &StructStats::GetChildStats(const BaseStatistics &stats, idx_t i) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::STRUCT_STATS);
	if (i >= StructType::GetChildCount(stats.GetType())) {
		throw InternalException("Calling StructStats::GetChildStats but there are no stats for this index");
	}
	return stats.child_stats[i];
}

BaseStatistics &StructStats::GetChildStats(BaseStatistics &stats, idx_t i) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::STRUCT_STATS);
	if (i >= StructType::GetChildCount(stats.GetType())) {
		throw InternalException("Calling StructStats::GetChildStats but there are no stats for this index");
	}
	return stats.child_stats[i];
}

void StructStats::SetChildStats(BaseStatistics &stats, idx_t i, const BaseStatistics &new_stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::STRUCT_STATS);
	D_ASSERT(i < StructType::GetChildCount(stats.GetType()));
	stats.child_stats[i].Copy(new_stats);
}

void StructStats::SetChildStats(BaseStatistics &stats, idx_t i, unique_ptr<BaseStatistics> new_stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::STRUCT_STATS);
	if (!new_stats) {
		StructStats::SetChildStats(stats, i,
		                           BaseStatistics::CreateUnknown(StructType::GetChildType(stats.GetType(), i)));
	} else {
		StructStats::SetChildStats(stats, i, *new_stats);
	}
}

void StructStats::Copy(BaseStatistics &stats, const BaseStatistics &other) {
	auto count = StructType::GetChildCount(stats.GetType());
	for (idx_t i = 0; i < count; i++) {
		stats.child_stats[i].Copy(other.child_stats[i]);
	}
}

void StructStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}
	D_ASSERT(stats.GetType() == other.GetType());
	auto child_count = StructType::GetChildCount(stats.GetType());
	for (idx_t i = 0; i < child_count; i++) {
		stats.child_stats[i].Merge(other.child_stats[i]);
	}
}

void StructStats::Serialize(const BaseStatistics &stats, FieldWriter &writer) {
	auto child_stats = StructStats::GetChildStats(stats);
	auto child_count = StructType::GetChildCount(stats.GetType());
	for (idx_t i = 0; i < child_count; i++) {
		writer.WriteSerializable(child_stats[i]);
	}
}

BaseStatistics StructStats::Deserialize(FieldReader &reader, LogicalType type) {
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	auto &child_types = StructType::GetChildTypes(type);
	BaseStatistics result(std::move(type));
	for (idx_t i = 0; i < child_types.size(); i++) {
		result.child_stats[i].Copy(
		    reader.ReadRequiredSerializable<BaseStatistics, BaseStatistics>(child_types[i].second));
	}
	return result;
}

string StructStats::ToString(const BaseStatistics &stats) {
	string result;
	result += " {";
	auto &child_types = StructType::GetChildTypes(stats.GetType());
	for (idx_t i = 0; i < child_types.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += child_types[i].first + ": " + stats.child_stats[i].ToString();
	}
	result += "}";
	return result;
}

void StructStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	auto &child_entries = StructVector::GetEntries(vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		stats.child_stats[i].Verify(*child_entries[i], sel, count);
	}
}

} // namespace duckdb
