#include "duckdb/storage/statistics/numeric_range_stats.hpp"

#include "duckdb/common/types/numeric_blob.hpp"
#include "duckdb/common/types/numeric_type.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

NumericRangeStats &NumericRangeStats::Get(const BaseStatistics &stats) {
	D_ASSERT(stats.GetType().id() == LogicalTypeId::NUMERIC);
	D_ASSERT(stats.extra_data);
	return stats.extra_data->Cast<NumericRangeStats>();
}

void NumericRangeStats::Construct(BaseStatistics &stats) {
	stats.extra_data = make_uniq<NumericRangeStats>();
}

void NumericRangeStats::Set(BaseStatistics &stats, const NumericRange &range) {
	auto &range_stats = Get(stats);
	range_stats.range = range;
	range_stats.has_range = true;
}

optional_ptr<const NumericRange> NumericRangeStats::TryGet(const BaseStatistics &stats) {
	auto &range_stats = Get(stats);
	if (!range_stats.has_range) {
		return nullptr;
	}
	return &range_stats.range;
}

LogicalType NumericRangeStats::GetRepresentation(const BaseStatistics &stats) {
	auto range = TryGet(stats);
	if (!range) {
		return LogicalType::INVALID;
	}
	return range->ResolveType();
}

void NumericRangeStats::Copy(BaseStatistics &stats, const BaseStatistics &other) {
	auto &range_stats = Get(stats);
	auto &other_stats = Get(other);
	range_stats.has_range = other_stats.has_range;
	range_stats.range = other_stats.range;
}

void NumericRangeStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	auto &range_stats = Get(stats);
	auto other_range = TryGet(other);
	if (!range_stats.has_range || !other_range) {
		// either side has values of unknown representation
		range_stats.has_range = false;
		return;
	}
	range_stats.range.Merge(*other_range);
}

void NumericRangeStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto range = TryGet(stats);
	serializer.WritePropertyWithDefault<bool>(104, "has_numeric_range", range != nullptr, false);
	if (range) {
		serializer.WriteObject(105, "numeric_range", [&](Serializer &object) { range->Serialize(object); });
	}
}

void NumericRangeStats::Deserialize(Deserializer &deserializer, BaseStatistics &stats) {
	auto has_range = deserializer.ReadPropertyWithExplicitDefault<bool>(104, "has_numeric_range", false);
	if (!has_range) {
		Get(stats).has_range = false;
		return;
	}
	NumericRange range;
	deserializer.ReadObject(105, "numeric_range",
	                        [&](Deserializer &object) { range = NumericRange::Deserialize(object); });
	Set(stats, range);
}

void NumericRangeStats::ToStruct(const BaseStatistics &stats, child_list_t<Value> &children) {
	auto range = TryGet(stats);
	if (!range) {
		return;
	}
	auto representation = range->ResolveType();
	children.emplace_back("representation",
	                      Value(NumericType::IsExact(representation) ? representation.ToString() : "CANONICAL"));
	if (range->HasValues()) {
		// the endpoints keep the representation they were collected in (an INTEGER or a DECIMAL of the row group that
		// holds them, which depends on the order the row groups merge): print the number, not the representation
		children.emplace_back("exact_min", Value(NumericBlob::NormalizeText(range->Min().ToString())));
		children.emplace_back("exact_max", Value(NumericBlob::NormalizeText(range->Max().ToString())));
	}
}

} // namespace duckdb
