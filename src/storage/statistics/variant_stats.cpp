#include "duckdb/storage/statistics/variant_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/variant.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include "duckdb/common/types/variant_visitor.hpp"

namespace duckdb {

void VariantColumnStatsData::SetType(VariantLogicalType type) {
	type_counts[static_cast<uint8_t>(type)]++;
}

VariantColumnStatsData &VariantColumnStatsData::GetOrCreateElement(VariantStatsData &stats) {
	if (element_stats == DConstants::INVALID_INDEX) {
		stats.columns.emplace_back();
		element_stats = stats.columns.size() - 1;
	}
	return stats.columns[element_stats];
}

VariantColumnStatsData &VariantColumnStatsData::GetOrCreateField(VariantStatsData &stats, const string &name) {
	auto it = field_stats.find(name);
	if (it == field_stats.end()) {
		stats.columns.emplace_back();
		it = field_stats.emplace(name, stats.columns.size() - 1).first;
	}
	return stats.columns[it->second];
}

void VariantStatsData::SetEmpty() {
	columns.resize(1);
}

void VariantStatsData::SetUnknown() {
	columns.resize(1);
}

void VariantStatsData::Merge(const VariantStatsData &other) {
	// throw NotImplementedException("VariantStatsData::Merge");
}

void VariantStatsData::Update(const Value &value) {
	// throw NotImplementedException("VariantStatsData::Update");
}

VariantColumnStatsData &VariantStatsData::GetColumnStats(idx_t index) {
	D_ASSERT(columns.size() > index);
	return columns[index];
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

namespace {

struct VariantStatsVisitor {
	using result_type = void;

	static void VisitNull(VariantStatsData &stats, VariantColumnStatsData &field_stats) {
		return;
	}
	static void VisitBoolean(bool val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
		return;
	}

	static void VisitMetadata(VariantLogicalType type_id, VariantStatsData &stats,
	                          VariantColumnStatsData &field_stats) {
		field_stats.SetType(type_id);
	}

	template <typename T>
	static void VisitInteger(T val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitFloat(float val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitDouble(double val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitUUID(hugeint_t val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitDate(date_t val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitInterval(interval_t val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitTime(dtime_t val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitTimeNanos(dtime_ns_t val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitTimeTZ(dtime_tz_t val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitTimestampSec(timestamp_sec_t val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitTimestampMs(timestamp_ms_t val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitTimestamp(timestamp_t val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitTimestampNanos(timestamp_ns_t val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitTimestampTZ(timestamp_tz_t val, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void WriteStringInternal(const string_t &str, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitString(const string_t &str, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitBlob(const string_t &blob, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitBignum(const string_t &bignum, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitGeometry(const string_t &geom, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}
	static void VisitBitstring(const string_t &bits, VariantStatsData &stats, VariantColumnStatsData &field_stats) {
	}

	template <typename T>
	static void VisitDecimal(T val, uint32_t width, uint32_t scale, VariantStatsData &stats,
	                         VariantColumnStatsData &field_stats) {
		//! FIXME: need to visit to be able to shred on DECIMAL values
	}

	static void VisitArray(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
	                       VariantStatsData &stats, VariantColumnStatsData &field_stats) {
		auto &element_stats = field_stats.GetOrCreateElement(stats);
		VariantVisitor<VariantStatsVisitor>::VisitArrayItems(variant, row, nested_data, stats, element_stats);
	}

	static void VisitObject(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
	                        VariantStatsData &stats, VariantColumnStatsData &field_stats) {
		//! Then visit the fields in sorted order
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto source_children_idx = nested_data.children_idx + i;

			//! Add the key of the field to the result
			auto keys_index = variant.GetKeysIndex(row, source_children_idx);
			auto &key = variant.GetKey(row, keys_index);

			auto &child_stats = field_stats.GetOrCreateField(stats, key.GetString());

			//! Visit the child value
			auto values_index = variant.GetValuesIndex(row, source_children_idx);
			VariantVisitor<VariantStatsVisitor>::Visit(variant, row, values_index, stats, child_stats);
		}
	}

	static void VisitDefault(VariantLogicalType type_id, const_data_ptr_t, VariantStatsData &stats,
	                         VariantColumnStatsData &field_stats) {
		throw InternalException("VariantLogicalType(%s) not handled", EnumUtil::ToString(type_id));
	}
};

} // namespace

void VariantStats::Update(BaseStatistics &stats, Vector &vector, idx_t count) {
	auto &data = GetDataUnsafe(stats);

	RecursiveUnifiedVectorFormat recursive_format;
	Vector::RecursiveToUnifiedFormat(vector, count, recursive_format);
	UnifiedVariantVectorData variant(recursive_format);

	auto &column_stats = data.GetColumnStats(0);
	for (idx_t i = 0; i < count; i++) {
		VariantVisitor<VariantStatsVisitor>::Visit(variant, i, 0, data, column_stats);
	}
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
