#include "duckdb/storage/statistics/variant_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

#include "duckdb/common/types/vector.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

void VariantStatsData::SetEmpty() {
	// throw NotImplementedException("VariantStatsData::SetEmpty");
}

void VariantStatsData::SetUnknown() {
	// throw NotImplementedException("VariantStatsData::SetUnknown");
}

void VariantStatsData::Merge(const VariantStatsData &other) {
	// throw NotImplementedException("VariantStatsData::Merge");
}

void VariantStatsData::Update(const Value &value) {
	// throw NotImplementedException("VariantStatsData::Update");
}

LogicalType VariantStats::GetUnshreddedType() {
	return LogicalType::STRUCT(StructType::GetChildTypes(LogicalType::VARIANT()));
}

void VariantStats::CreateUnshreddedStats(BaseStatistics &stats) {
	BaseStatistics::Construct(stats.child_stats[0], GetUnshreddedType());
}

void VariantStats::Construct(BaseStatistics &stats) {
	stats.child_stats = unsafe_unique_array<BaseStatistics>(new BaseStatistics[1]);
	CreateUnshreddedStats(stats);
}

BaseStatistics VariantStats::CreateUnknown(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	GetDataUnsafe(result).SetUnknown();
	result.child_stats[0].Copy(BaseStatistics::CreateUnknown(GetUnshreddedType()));
	return result;
}

BaseStatistics VariantStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	GetDataUnsafe(result).SetEmpty();
	result.child_stats[0].Copy(BaseStatistics::CreateEmpty(GetUnshreddedType()));
	return result;
}

const BaseStatistics &VariantStats::GetUnshreddedStats(const BaseStatistics &stats) {
	if (stats.GetStatsType() != StatisticsType::VARIANT_STATS) {
		throw InternalException("Calling VariantStats::GetChildStats on stats that is not a variant");
	}
	return stats.child_stats[0];
}

BaseStatistics &VariantStats::GetUnshreddedStats(BaseStatistics &stats) {
	if (stats.GetStatsType() != StatisticsType::VARIANT_STATS) {
		throw InternalException("Calling VariantStats::GetChildStats on stats that is not a variant");
	}
	return stats.child_stats[0];
}

void VariantStats::SetUnshreddedStats(BaseStatistics &stats, const BaseStatistics &new_stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::VARIANT_STATS);
	stats.child_stats[0].Copy(new_stats);
}

void VariantStats::SetUnshreddedStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats) {
	if (stats.GetStatsType() != StatisticsType::VARIANT_STATS) {
		throw InternalException("Calling VariantStats::GetChildStats on stats that is not a variant");
	}
	if (!new_stats) {
		CreateUnshreddedStats(stats);
	} else {
		SetUnshreddedStats(stats, *new_stats);
	}
}

// void VariantStats::UpdateFromVector(Vector &vector, idx_t count) {
//	RecursiveUnifiedVectorFormat recursive_format;
//	Vector::RecursiveToUnifiedFormat(vector, count, recursive_format);
//	UnifiedVariantVectorData variant(recursive_format);

//	for (idx_t i = 0; i < count; i++) {
//		stats_data.total_count++;

//		if (!variant.RowIsValid(i)) {
//			stats_data.null_count++;
//			continue;
//		}

//		AnalyzeVariantValue(variant, i, 0, stats_data);
//	}
//}

// void AnalyzeVariantValue(const UnifiedVariantVectorData &variant, idx_t row, uint32_t values_index,
//                         VariantStatsData &stats) {
//	auto type_id = variant.GetTypeId(row, values_index);
//	stats.type_counts[static_cast<uint8_t>(type_id)]++;

//	switch (type_id) {
//	case VariantLogicalType::OBJECT: {
//		if (!stats.object_stats) {
//			stats.object_stats = make_uniq<ObjectStatsData>();
//		}

//		auto nested_data = VariantUtils::DecodeNestedData(variant, row, values_index);
//		for (idx_t i = 0; i < nested_data.child_count; i++) {
//			auto keys_index = variant.GetKeysIndex(row, i + nested_data.children_idx);
//			auto child_values_index = variant.GetValuesIndex(row, i + nested_data.children_idx);
//			auto &key = variant.GetKey(row, keys_index);

//			auto &field_stats = stats.object_stats->field_stats[key.GetString()];
//			stats.object_stats->field_frequencies[key.GetString()]++;

//			AnalyzeVariantValue(variant, row, child_values_index, field_stats);
//		}
//		break;
//	}
//	case VariantLogicalType::ARRAY: {
//		if (!stats.array_stats) {
//			stats.array_stats = make_uniq<ArrayStatsData>();
//		}

//		auto nested_data = VariantUtils::DecodeNestedData(variant, row, values_index);
//		stats.array_stats->size_distribution[nested_data.child_count]++;

//		for (idx_t i = 0; i < nested_data.child_count; i++) {
//			auto child_values_index = variant.GetValuesIndex(row, i + nested_data.children_idx);
//			AnalyzeVariantValue(variant, row, child_values_index, stats.array_stats->element_stats);
//		}
//		break;
//	}
//	case VariantLogicalType::DECIMAL: {
//		auto decimal_data = VariantUtils::DecodeDecimalData(variant, row, values_index);
//		auto physical_type = decimal_data.GetPhysicalType();
//		switch (physical_type) {
//		case PhysicalType::INT16:
//			stats.decimal_physical_types[0]++;
//			break;
//		case PhysicalType::INT32:
//			stats.decimal_physical_types[1]++;
//			break;
//		case PhysicalType::INT64:
//			stats.decimal_physical_types[2]++;
//			break;
//		default:
//			break;
//		}
//		break;
//	}
//	default:
//		// Primitive types already counted above
//		break;
//	}
//}

// LogicalType VariantStats::GetOptimalShreddedType(double shredding_threshold) const {
//	// Determine if we should shred based on type distribution
//	auto total_non_null = stats_data.total_count - stats_data.null_count;
//	if (total_non_null == 0) {
//		return LogicalType::VARIANT();
//	}

//	// Check for dominant object pattern
//	auto object_count = stats_data.type_counts[static_cast<uint8_t>(VariantLogicalType::OBJECT)];
//	if (object_count > 0 && stats_data.object_stats && (double)object_count / total_non_null >= shredding_threshold) {

//		// Build struct type from frequent fields
//		child_list_t<LogicalType> struct_fields;
//		for (auto &field : stats_data.object_stats->field_stats) {
//			auto field_frequency = stats_data.object_stats->field_frequencies.at(field.first);
//			if ((double)field_frequency / object_count >= shredding_threshold) {
//				// Recursively determine optimal type for this field
//				VariantStats field_stats(LogicalType::VARIANT());
//				field_stats.stats_data = field.second;
//				auto field_type = field_stats.GetOptimalShreddedType(shredding_threshold);
//				struct_fields.emplace_back(field.first, field_type);
//			}
//		}

//		if (!struct_fields.empty()) {
//			return LogicalType::STRUCT(struct_fields);
//		}
//	}

//	// Check for dominant array pattern
//	auto array_count = stats_data.type_counts[static_cast<uint8_t>(VariantLogicalType::ARRAY)];
//	if (array_count > 0 && stats_data.array_stats && (double)array_count / total_non_null >= shredding_threshold) {

//		VariantStats element_stats(LogicalType::VARIANT());
//		element_stats.stats_data = stats_data.array_stats->element_stats;
//		auto element_type = element_stats.GetOptimalShreddedType(shredding_threshold);

//		if (element_type.id() != LogicalTypeId::VARIANT) {
//			return LogicalType::LIST(element_type);
//		}
//	}

//	// Check for dominant primitive type
//	for (idx_t i = 0; i < stats_data.type_counts.size(); i++) {
//		if (i == static_cast<uint8_t>(VariantLogicalType::OBJECT) ||
//		    i == static_cast<uint8_t>(VariantLogicalType::ARRAY)) {
//			continue;
//		}

//		auto type_count = stats_data.type_counts[i];
//		if ((double)type_count / total_non_null >= shredding_threshold) {
//			return GetLogicalTypeFromVariantType(static_cast<VariantLogicalType>(i));
//		}
//	}

//	return LogicalType::VARIANT(); // No clear shredding pattern
//}

void VariantStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &unshredded_stats = VariantStats::GetUnshreddedStats(stats);

	serializer.WriteList(200, "child_stats", 1,
	                     [&](Serializer::List &list, idx_t i) { list.WriteElement(unshredded_stats); });
}

void VariantStats::Deserialize(Deserializer &deserializer, BaseStatistics &base) {
	auto &type = base.GetType();
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	D_ASSERT(type.id() == LogicalTypeId::VARIANT);

	auto unshredded_type = GetUnshreddedType();
	deserializer.ReadList(200, "child_stats", [&](Deserializer::List &list, idx_t i) {
		deserializer.Set<const LogicalType &>(unshredded_type);
		auto stat = list.ReadElement<BaseStatistics>();
		base.child_stats[i].Copy(stat);
		deserializer.Unset<LogicalType>();
	});
}

string VariantStats::ToString(const BaseStatistics &stats) {
	const auto &data = GetDataUnsafe(stats);
	string result;

	throw NotImplementedException("VariantStats::ToString");
	return result;
}

void VariantStats::Update(BaseStatistics &stats, const Value &value) {
	auto &data = GetDataUnsafe(stats);
	data.Update(value);
}

void VariantStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}
	//! Merge the unshredded stats
	stats.child_stats[0].Merge(other.child_stats[0]);
}

void VariantStats::Copy(BaseStatistics &stats, const BaseStatistics &other) {
	stats.child_stats[0].Copy(other.child_stats[0]);
}

void VariantStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	// TODO: Verify stats
}

const VariantStatsData &VariantStats::GetDataUnsafe(const BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::VARIANT_STATS);
	return stats.variant_data;
}

VariantStatsData &VariantStats::GetDataUnsafe(BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::VARIANT_STATS);
	return stats.variant_data;
}

} // namespace duckdb
