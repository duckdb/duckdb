#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StructStats::CreateEmpty(LogicalType type) {
	auto &child_types = StructType::GetChildTypes(type);
	auto result = make_unique<BaseStatistics>(std::move(type));
	result->InitializeBase();
	for (auto &entry : child_types) {
		result->child_stats.push_back(BaseStatistics::CreateEmpty(entry.second));
	}
	return result;
}

const vector<unique_ptr<BaseStatistics>> &StructStats::GetChildStats(const BaseStatistics &stats) {
	return stats.child_stats;
}

const BaseStatistics &StructStats::GetChildStats(const BaseStatistics &stats, idx_t i) {
	if (i >= stats.child_stats.size() || !stats.child_stats[i]) {
		throw InternalException("Calling StructStats::GetChildStats but there are no stats for this index");
	}
	return *stats.child_stats[i];
}

BaseStatistics &StructStats::GetChildStats(BaseStatistics &stats, idx_t i) {
	if (i >= stats.child_stats.size() || !stats.child_stats[i]) {
		throw InternalException("Calling StructStats::GetChildStats but there are no stats for this index");
	}
	return *stats.child_stats[i];
}

void StructStats::SetChildStats(BaseStatistics &stats, idx_t i, unique_ptr<BaseStatistics> new_stats) {
	D_ASSERT(i < stats.child_stats.size());
	stats.child_stats[i] = std::move(new_stats);
}

bool StructStats::IsStruct(const BaseStatistics &stats) {
	return stats.GetType().InternalType() == PhysicalType::STRUCT;
}

void StructStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}

	D_ASSERT(other.child_stats.size() == stats.child_stats.size());
	for (idx_t i = 0; i < other.child_stats.size(); i++) {
		if (stats.child_stats[i] && other.child_stats[i]) {
			stats.child_stats[i]->Merge(*other.child_stats[i]);
		} else {
			stats.child_stats[i].reset();
		}
	}
}

void StructStats::Serialize(const BaseStatistics &stats, FieldWriter &writer) {
	auto &child_stats = StructStats::GetChildStats(stats);
	for (auto &child_stat : child_stats) {
		writer.WriteOptional(child_stat);
	}
}

unique_ptr<BaseStatistics> StructStats::Deserialize(FieldReader &reader, LogicalType type) {
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	auto &child_types = StructType::GetChildTypes(type);
	auto result = make_unique<BaseStatistics>(std::move(type));
	for (auto &entry : child_types) {
		result->child_stats.push_back(reader.ReadOptional<BaseStatistics>(nullptr, entry.second));
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
		result += child_types[i].first + ": " + (stats.child_stats[i] ? stats.child_stats[i]->ToString() : "No Stats");
	}
	result += "}";
	return result;
}

void StructStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	auto &child_entries = StructVector::GetEntries(vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		if (stats.child_stats[i]) {
			stats.child_stats[i]->Verify(*child_entries[i], sel, count);
		}
	}
}

} // namespace duckdb
