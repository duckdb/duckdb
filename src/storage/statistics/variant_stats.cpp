#include "duckdb/storage/statistics/variant_stats.hpp"
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
	auto &variant_stats = GetDataUnsafe(result);
	variant_stats.is_shredded = true;
	result.child_stats[0].Copy(BaseStatistics::CreateEmpty(VariantShredding::GetUnshreddedType()));
	BaseStatistics::Construct(result.child_stats[1], shredded_type);
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
	AssertVariant(stats);
	auto &data = GetDataUnsafe(stats);
	if (!data.is_shredded) {
		BaseStatistics::Construct(stats.child_stats[1], new_stats.GetType());
		data.is_shredded = true;
	}
	stats.child_stats[1].Copy(new_stats);
}

void VariantStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &unshredded_stats = VariantStats::GetUnshreddedStats(stats);

	serializer.WriteList(200, "child_stats", 1,
	                     [&](Serializer::List &list, idx_t i) { list.WriteElement(unshredded_stats); });
}

void VariantStats::Deserialize(Deserializer &deserializer, BaseStatistics &base) {
	auto &type = base.GetType();
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	D_ASSERT(type.id() == LogicalTypeId::VARIANT);

	auto unshredded_type = VariantShredding::GetUnshreddedType();
	deserializer.ReadList(200, "child_stats", [&](Deserializer::List &list, idx_t i) {
		deserializer.Set<const LogicalType &>(unshredded_type);
		auto stat = list.ReadElement<BaseStatistics>();
		base.child_stats[i].Copy(stat);
		deserializer.Unset<LogicalType>();
	});
}

string VariantStats::ToString(const BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	return StringUtil::Format("is_shredded: %s", data.is_shredded ? "true" : "false");
}

void VariantStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}
	auto &data = GetDataUnsafe(stats);
	auto &other_data = GetDataUnsafe(other);

	D_ASSERT(!data.is_shredded && !other_data.is_shredded);
	for (idx_t i = 0; i < 1; i++) {
		stats.child_stats[i].Merge(other.child_stats[i]);
	}
}

void VariantStats::Copy(BaseStatistics &stats, const BaseStatistics &other) {
	stats.child_stats[0].Copy(other.child_stats[0]);
	auto &data = VariantStats::GetDataUnsafe(stats);
	auto &other_data = VariantStats::GetDataUnsafe(other);
	if (other_data.is_shredded) {
		if (!data.is_shredded) {
			BaseStatistics::Construct(stats.child_stats[1], other.child_stats[1].GetType());
			data.is_shredded = true;
		}
		//! FIXME: assumes equal shredding type?
		stats.child_stats[1].Copy(other.child_stats[1]);
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
