#include "duckdb/storage/statistics/variant_stats.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/variant.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include "duckdb/common/types/variant_visitor.hpp"
#include "duckdb/function/variant/variant_shredding.hpp"

namespace duckdb {

static void AssertVariant(const BaseStatistics &stats) {
	if (DUCKDB_UNLIKELY(stats.GetStatsType() != StatisticsType::VARIANT_STATS)) {
		throw InternalException(
		    "Calling a VariantStats method on BaseStatistics that are not of type VARIANT, but of type %s",
		    EnumUtil::ToString(stats.GetStatsType()));
	}
}

void VariantStats::CreateUnshreddedStats(BaseStatistics &stats) {
	BaseStatistics::Construct(stats.child_stats[0], VariantShredding::GetUnshreddedType());
}

void VariantStats::CreateShreddedStats(BaseStatistics &stats, const LogicalType &shredded_type) {
	BaseStatistics::Construct(stats.child_stats[1], shredded_type);
	auto &data = GetDataUnsafe(stats);
	data.is_shredded = true;
}

void VariantStats::Construct(BaseStatistics &stats) {
	stats.child_stats = unsafe_unique_array<BaseStatistics>(new BaseStatistics[2]);
	GetDataUnsafe(stats).is_shredded = false;
	CreateUnshreddedStats(stats);
}

BaseStatistics VariantStats::CreateUnknown(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	GetDataUnsafe(result).is_shredded = false;
	result.child_stats[0].Copy(BaseStatistics::CreateUnknown(VariantShredding::GetUnshreddedType()));
	return result;
}

BaseStatistics VariantStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	GetDataUnsafe(result).is_shredded = false;
	result.child_stats[0].Copy(BaseStatistics::CreateEmpty(VariantShredding::GetUnshreddedType()));
	return result;
}

BaseStatistics VariantStats::CreateShredded(const LogicalType &shredded_type) {
	BaseStatistics result(LogicalType::VARIANT());
	result.InitializeEmpty();

	CreateShreddedStats(result, shredded_type);
	result.child_stats[0].Copy(BaseStatistics::CreateEmpty(VariantShredding::GetUnshreddedType()));
	result.child_stats[1].Copy(BaseStatistics::CreateEmpty(shredded_type));
	return result;
}

const BaseStatistics &VariantStats::GetShreddedStats(const BaseStatistics &stats) {
	AssertVariant(stats);
	D_ASSERT(GetDataUnsafe(stats).is_shredded == true);
	return stats.child_stats[1];
}

BaseStatistics &VariantStats::GetShreddedStats(BaseStatistics &stats) {
	AssertVariant(stats);
	D_ASSERT(GetDataUnsafe(stats).is_shredded == true);
	return stats.child_stats[1];
}

const BaseStatistics &VariantStats::GetUnshreddedStats(const BaseStatistics &stats) {
	AssertVariant(stats);
	return stats.child_stats[0];
}

BaseStatistics &VariantStats::GetUnshreddedStats(BaseStatistics &stats) {
	AssertVariant(stats);
	return stats.child_stats[0];
}

void VariantStats::SetUnshreddedStats(BaseStatistics &stats, const BaseStatistics &new_stats) {
	AssertVariant(stats);
	stats.child_stats[0].Copy(new_stats);
}

void VariantStats::SetUnshreddedStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats) {
	AssertVariant(stats);
	if (!new_stats) {
		CreateUnshreddedStats(stats);
	} else {
		SetUnshreddedStats(stats, *new_stats);
	}
}

void VariantStats::SetShreddedStats(BaseStatistics &stats, const BaseStatistics &new_stats) {
	auto &data = GetDataUnsafe(stats);
	if (!data.is_shredded) {
		BaseStatistics::Construct(stats.child_stats[1], new_stats.GetType());
		data.is_shredded = true;
	}
	stats.child_stats[1].Copy(new_stats);
}

void VariantStats::SetShreddedStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats) {
	AssertVariant(stats);
	D_ASSERT(new_stats);
	SetShreddedStats(stats, *new_stats);
}

void VariantStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &data = GetDataUnsafe(stats);
	auto &unshredded_stats = VariantStats::GetUnshreddedStats(stats);

	serializer.WriteProperty(200, "is_shredded", data.is_shredded);

	serializer.WriteProperty(225, "unshredded_stats", unshredded_stats);
	if (data.is_shredded) {
		auto &shredded_stats = VariantStats::GetShreddedStats(stats);
		serializer.WriteProperty(230, "shredded_type", shredded_stats.type);
		serializer.WriteProperty(235, "shredded_stats", shredded_stats);
	}
}

void VariantStats::Deserialize(Deserializer &deserializer, BaseStatistics &base) {
	auto &type = base.GetType();
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	D_ASSERT(type.id() == LogicalTypeId::VARIANT);
	auto &data = GetDataUnsafe(base);

	auto unshredded_type = VariantShredding::GetUnshreddedType();
	data.is_shredded = deserializer.ReadProperty<bool>(200, "is_shredded");

	{
		//! Read the 'unshredded_stats' child
		deserializer.Set<const LogicalType &>(unshredded_type);
		auto stat = deserializer.ReadProperty<BaseStatistics>(225, "unshredded_stats");
		base.child_stats[0].Copy(stat);
		deserializer.Unset<LogicalType>();
	}

	if (!data.is_shredded) {
		return;
	}
	//! Read the type of the 'shredded_stats'
	auto shredded_type = deserializer.ReadProperty<LogicalType>(230, "shredded_type");

	{
		//! Finally read the 'shredded_stats' themselves
		deserializer.Set<const LogicalType &>(shredded_type);
		auto stat = deserializer.ReadProperty<BaseStatistics>(235, "shredded_stats");
		if (base.child_stats[1].type.id() == LogicalTypeId::INVALID) {
			base.child_stats[1] = BaseStatistics::CreateUnknown(shredded_type);
		}
		base.child_stats[1].Copy(stat);
		deserializer.Unset<LogicalType>();
	}
}

LogicalType ToStructuredType(const LogicalType &shredding) {
	D_ASSERT(shredding.id() == LogicalTypeId::STRUCT);
	auto &child_types = StructType::GetChildTypes(shredding);
	D_ASSERT(child_types.size() == 2);

	auto &typed_value = child_types[1].second;

	if (typed_value.id() == LogicalTypeId::STRUCT) {
		auto &struct_children = StructType::GetChildTypes(typed_value);
		child_list_t<LogicalType> structured_children;
		for (auto &child : struct_children) {
			structured_children.emplace_back(child.first, ToStructuredType(child.second));
		}
		return LogicalType::STRUCT(structured_children);
	} else if (typed_value.id() == LogicalTypeId::LIST) {
		auto &child_type = ListType::GetChildType(typed_value);
		return LogicalType::LIST(ToStructuredType(child_type));
	} else {
		return typed_value;
	}
}

string VariantStats::ToString(const BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	string result;
	result = StringUtil::Format("is_shredded: %s", data.is_shredded ? "true" : "false");
	if (data.is_shredded) {
		result += ", shredding: {";
		result += StringUtil::Format("typed_value_type: %s, ", ToStructuredType(stats.child_stats[1].type).ToString());
		result += StringUtil::Format("stats: %s", stats.child_stats[1].ToString());
		result += "}";
	}
	return result;
}

bool VariantStats::MergeShredding(BaseStatistics &stats, const BaseStatistics &other) {
	//! shredded_type:
	//! STRUCT(untyped_value_index UINTEGER, typed_value <shredding>)

	//! shredding, 1 of:
	//! - <primitive type>
	//! - <shredded_type>
	//! - <shredded_type>[]

	D_ASSERT(stats.type.id() == LogicalTypeId::STRUCT);
	D_ASSERT(other.type.id() == LogicalTypeId::STRUCT);

	auto &stats_children = StructType::GetChildTypes(stats.type);
	auto &other_children = StructType::GetChildTypes(other.type);
	D_ASSERT(stats_children.size() == 2);
	D_ASSERT(other_children.size() == 2);

	auto &stats_typed_value_type = stats_children[1].second;
	auto &other_typed_value_type = other_children[1].second;

	auto &stats_typed_value = StructStats::GetChildStats(stats, 1);
	auto &other_typed_value = StructStats::GetChildStats(other, 1);

	if (stats_typed_value_type.id() == LogicalTypeId::STRUCT) {
		if (stats_typed_value_type.id() != other_typed_value_type.id()) {
			//! other is not an OBJECT, can't merge
			return false;
		}
		auto &stats_object_children = StructType::GetChildTypes(stats_typed_value_type);
		auto &other_object_children = StructType::GetChildTypes(other_typed_value_type);
		if (stats_object_children.size() != other_object_children.size()) {
			//! FIXME: implement merging of overlapping children
			return false;
		}
		for (idx_t i = 0; i < stats_object_children.size(); i++) {
			auto &stats_object_child = stats_object_children[i];
			auto &other_object_child = other_object_children[i];

			if (stats_object_child.first != other_object_child.first) {
				//! FIXME: implement merging of overlapping children
				return false;
			}

			auto &stats_child = StructStats::GetChildStats(stats_typed_value, i);
			auto &other_child = StructStats::GetChildStats(other_typed_value, i);
			if (!MergeShredding(stats_child, other_child)) {
				return false;
			}
		}
		return true;
	} else if (stats_typed_value_type.id() == LogicalTypeId::LIST) {
		if (stats_typed_value_type.id() != other_typed_value_type.id()) {
			//! other is not an ARRAY, can't merge
			return false;
		}
		auto &stats_child = ListStats::GetChildStats(stats_typed_value);
		auto &other_child = ListStats::GetChildStats(other_typed_value);
		return MergeShredding(stats_child, other_child);
	} else {
		D_ASSERT(!stats_typed_value_type.IsNested());
		if (stats_typed_value_type.id() != other_typed_value_type.id()) {
			//! other is not the same type, can't merge
			return false;
		}
		stats_typed_value.Merge(other_typed_value);
		return true;
	}
}

void VariantStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}

	stats.child_stats[0].Merge(other.child_stats[0]);
	auto &data = GetDataUnsafe(stats);
	auto &other_data = GetDataUnsafe(other);

	if (other_data.is_shredded) {
		if (!data.is_shredded) {
			stats.child_stats[1] = BaseStatistics::CreateUnknown(other.child_stats[1].GetType());
			stats.child_stats[1].Copy(other.child_stats[1]);
			data.is_shredded = true;
		} else {
			//! FIXME: assumes equal shredding type?
			if (!MergeShredding(stats.child_stats[1], other.child_stats[1])) {
				data.is_shredded = false;
			}
		}
	}
}

void VariantStats::Copy(BaseStatistics &stats, const BaseStatistics &other) {
	auto &other_data = VariantStats::GetDataUnsafe(other);

	stats.child_stats[0].Copy(other.child_stats[0]);
	if (other_data.is_shredded) {
		stats.child_stats[1] = BaseStatistics::CreateUnknown(other.child_stats[1].GetType());
		stats.child_stats[1].Copy(other.child_stats[1]);
	} else {
		stats.child_stats[1].type = LogicalType::INVALID;
	}
}

void VariantStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	// TODO: Verify stats
}

const VariantStatsData &VariantStats::GetDataUnsafe(const BaseStatistics &stats) {
	AssertVariant(stats);
	return stats.stats_union.variant_data;
}

VariantStatsData &VariantStats::GetDataUnsafe(BaseStatistics &stats) {
	AssertVariant(stats);
	return stats.stats_union.variant_data;
}

} // namespace duckdb
