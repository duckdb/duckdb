#include "duckdb/storage/statistics/unbounded_numeric_stats.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/numeric_type.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

namespace duckdb {

UnboundedNumericStatsData &UnboundedNumericStats::GetData(BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::UNBOUNDED_NUMERIC_STATS);
	if (!stats.extra_data) {
		stats.extra_data = make_uniq<UnboundedNumericStatsData>();
	}
	return stats.extra_data->Cast<UnboundedNumericStatsData>();
}

const UnboundedNumericStatsData &UnboundedNumericStats::GetData(const BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::UNBOUNDED_NUMERIC_STATS);
	if (!stats.extra_data) {
		throw InternalException("UnboundedNumericStats::GetData called on statistics without data");
	}
	return stats.extra_data->Cast<UnboundedNumericStatsData>();
}

BaseStatistics UnboundedNumericStats::CreateUnknown(LogicalType type) {
	BaseStatistics result(std::move(type));
	auto &data = GetData(result);
	data.unknown = true;
	return result;
}

BaseStatistics UnboundedNumericStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	GetData(result);
	return result;
}

void UnboundedNumericStats::Copy(BaseStatistics &stats, const BaseStatistics &other) {
	stats.extra_data = make_uniq<UnboundedNumericStatsData>(GetData(other));
}

void UnboundedNumericStats::SetUnknown(BaseStatistics &stats) {
	auto &data = GetData(stats);
	data.unknown = true;
	data.has_min_max = false;
	data.min = Value();
	data.max = Value();
}

bool UnboundedNumericStats::HasMinMax(const BaseStatistics &stats) {
	auto &data = GetData(stats);
	return data.has_min_max && !data.unknown;
}

bool UnboundedNumericStats::IsUnknown(const BaseStatistics &stats) {
	return GetData(stats).unknown;
}

uint8_t UnboundedNumericStats::GetScale(const BaseStatistics &stats) {
	return GetData(stats).scale;
}

const Value &UnboundedNumericStats::Min(const BaseStatistics &stats) {
	return GetData(stats).min;
}

const Value &UnboundedNumericStats::Max(const BaseStatistics &stats) {
	return GetData(stats).max;
}

//! The value as DECIMAL(width, scale) with the smallest width that holds it; false when it needs more than the maximum
//! number of digits
static bool Normalize(const Value &value, uint8_t scale, Value &result) {
	idx_t width = NumericType::IntegerDigits(value) + scale;
	if (width > Decimal::MAX_WIDTH_DECIMAL) {
		return false;
	}
	if (width < 1) {
		width = 1;
	}
	auto target = LogicalType::DECIMAL(NumericCast<uint8_t>(width), scale);
	if (value.type() == target) {
		result = value;
		return true;
	}
	result = value.DefaultCastAs(target);
	return true;
}

//! Bring both values to the same DECIMAL type so they compare exactly
static void Align(Value &left, Value &right) {
	auto left_width = DecimalType::GetWidth(left.type());
	auto right_width = DecimalType::GetWidth(right.type());
	D_ASSERT(DecimalType::GetScale(left.type()) == DecimalType::GetScale(right.type()));
	if (left_width == right_width) {
		return;
	}
	auto target = LogicalType::DECIMAL(MaxValue(left_width, right_width), DecimalType::GetScale(left.type()));
	if (left_width < right_width) {
		left = left.DefaultCastAs(target);
	} else {
		right = right.DefaultCastAs(target);
	}
}

void UnboundedNumericStats::MergeRange(UnboundedNumericStatsData &data, const UnboundedNumericStatsData &other) {
	if (data.unknown) {
		return;
	}
	if (other.unknown) {
		data.unknown = true;
		data.has_min_max = false;
		data.min = Value();
		data.max = Value();
		return;
	}
	if (!other.has_min_max) {
		data.scale = MaxValue(data.scale, other.scale);
		return;
	}
	auto scale = MaxValue(data.scale, other.scale);
	Value other_min, other_max;
	if (!Normalize(other.min, scale, other_min) || !Normalize(other.max, scale, other_max)) {
		data.unknown = true;
		data.has_min_max = false;
		data.min = Value();
		data.max = Value();
		return;
	}
	if (!data.has_min_max) {
		data.scale = scale;
		data.min = std::move(other_min);
		data.max = std::move(other_max);
		data.has_min_max = true;
		return;
	}
	Value min, max;
	if (!Normalize(data.min, scale, min) || !Normalize(data.max, scale, max)) {
		data.unknown = true;
		data.has_min_max = false;
		data.min = Value();
		data.max = Value();
		return;
	}
	Align(min, other_min);
	if (ValueOperations::LessThan(other_min, min)) {
		min = other_min;
	}
	Align(max, other_max);
	if (ValueOperations::GreaterThan(other_max, max)) {
		max = other_max;
	}
	data.scale = scale;
	data.min = std::move(min);
	data.max = std::move(max);
}

void UnboundedNumericStats::Update(BaseStatistics &stats, const Value &value) {
	if (value.IsNull()) {
		return;
	}
	if (!NumericType::IsExact(value.type())) {
		throw InternalException("UnboundedNumericStats::Update called with a value of type %s",
		                        value.type().ToString());
	}
	UnboundedNumericStatsData other;
	other.has_min_max = true;
	other.scale = value.type().id() == LogicalTypeId::DECIMAL ? DecimalType::GetScale(value.type()) : 0;
	other.min = value;
	other.max = value;
	MergeRange(GetData(stats), other);
}

//! The extreme values a DECIMAL type can hold
static void DecimalTypeBounds(const LogicalType &type, Value &min, Value &max) {
	auto width = DecimalType::GetWidth(type);
	auto scale = DecimalType::GetScale(type);
	string digits(width - scale, '9');
	if (scale > 0) {
		digits += "." + string(scale, '9');
	}
	if (digits.empty() || digits[0] == '.') {
		digits = "0" + digits;
	}
	max = Value(digits).DefaultCastAs(type);
	min = Value("-" + digits).DefaultCastAs(type);
}

BaseStatistics UnboundedNumericStats::FromTyped(const BaseStatistics &typed_stats) {
	auto result = CreateEmpty(LogicalType::NUMERIC);
	result.CopyBase(typed_stats);
	auto &type = typed_stats.GetType();
	if (!NumericType::IsExact(type)) {
		throw InternalException("UnboundedNumericStats::FromTyped called on statistics of type %s", type.ToString());
	}
	auto &data = GetData(result);
	data.scale = type.id() == LogicalTypeId::DECIMAL ? DecimalType::GetScale(type) : 0;
	UnboundedNumericStatsData other;
	other.scale = data.scale;
	switch (typed_stats.GetStatsType()) {
	case StatisticsType::NUMERIC_STATS:
		if (!NumericStats::HasMinMax(typed_stats)) {
			return result;
		}
		other.has_min_max = true;
		other.min = NumericStats::Min(typed_stats);
		other.max = NumericStats::Max(typed_stats);
		break;
	case StatisticsType::STRING_STATS: {
		// wide decimals keep their BIGNUM blobs as string statistics
		other.has_min_max = true;
		if (StringStats::GetMinType(typed_stats) == StringStatsType::EXACT_STATS &&
		    StringStats::GetMaxType(typed_stats) == StringStatsType::EXACT_STATS) {
			other.min = Value::WIDEDECIMAL(StringStats::Min(typed_stats), DecimalType::GetWidth(type), data.scale);
			other.max = Value::WIDEDECIMAL(StringStats::Max(typed_stats), DecimalType::GetWidth(type), data.scale);
		} else {
			// the exact values are not known - the type bounds cover them
			DecimalTypeBounds(type, other.min, other.max);
		}
		break;
	}
	default:
		throw InternalException("UnboundedNumericStats::FromTyped called on unexpected statistics of type %s",
		                        type.ToString());
	}
	MergeRange(data, other);
	return result;
}

void UnboundedNumericStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	switch (other.GetStatsType()) {
	case StatisticsType::UNBOUNDED_NUMERIC_STATS:
		MergeRange(GetData(stats), GetData(other));
		break;
	case StatisticsType::NUMERIC_STATS:
	case StatisticsType::STRING_STATS:
		if (!NumericType::IsExact(other.GetType())) {
			throw InternalException("Cannot merge statistics of type %s into NUMERIC statistics",
			                        other.GetType().ToString());
		}
		MergeRange(GetData(stats), GetData(FromTyped(other)));
		break;
	default:
		// validity and NULL statistics carry no value range
		break;
	}
}

//! Whether the value survives a round trip through the type, i.e. the type can hold it exactly
static bool TryExactCast(const Value &value, const LogicalType &type, Value &result) {
	auto cast = value.DefaultTryCastAs(type, nullptr, true);
	if (!cast) {
		return false;
	}
	auto back = cast->DefaultTryCastAs(value.type(), nullptr, true);
	if (!back || !ValueOperations::Equals(*back, value)) {
		return false;
	}
	result = *cast;
	return true;
}

BaseStatistics UnboundedNumericStats::ToTyped(const BaseStatistics &stats, const LogicalType &type) {
	auto &data = GetData(stats);
	Value min, max;
	if (data.unknown ||
	    (data.has_min_max && (!TryExactCast(data.min, type, min) || !TryExactCast(data.max, type, max)))) {
		auto result = BaseStatistics::CreateUnknown(type);
		result.CopyBase(stats);
		return result;
	}
	if (BaseStatistics::GetStatsType(type) == StatisticsType::STRING_STATS) {
		// wide decimals keep their BIGNUM blobs as string statistics
		auto result = StringStats::CreateEmpty(type);
		result.CopyBase(stats);
		if (data.has_min_max) {
			auto &min_blob = StringValue::Get(min);
			auto &max_blob = StringValue::Get(max);
			StringStats::FromConstant(result, string_t(min_blob));
			StringStats::MergeInConstant(result, string_t(max_blob));
		}
		return result;
	}
	auto result = NumericStats::CreateEmpty(type);
	result.CopyBase(stats);
	if (data.has_min_max) {
		NumericStats::SetMin(result, min);
		NumericStats::SetMax(result, max);
	}
	return result;
}

LogicalType UnboundedNumericStats::ResolveType(const BaseStatistics &stats) {
	auto &data = GetData(stats);
	if (data.unknown) {
		throw InternalException("Cannot resolve the representation of a NUMERIC column whose statistics are unknown");
	}
	if (!data.has_min_max) {
		return NumericType::DefaultType();
	}
	return NumericType::ForRange(data.min, data.max, data.scale);
}

void UnboundedNumericStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &data = GetData(stats);
	serializer.WriteProperty(200, "has_min_max", data.has_min_max);
	serializer.WriteProperty(201, "unknown", data.unknown);
	serializer.WriteProperty(202, "scale", data.scale);
	if (data.has_min_max) {
		serializer.WriteProperty(203, "min", data.min);
		serializer.WriteProperty(204, "max", data.max);
	}
}

void UnboundedNumericStats::Deserialize(Deserializer &deserializer, BaseStatistics &stats) {
	auto &data = GetData(stats);
	data.has_min_max = deserializer.ReadProperty<bool>(200, "has_min_max");
	data.unknown = deserializer.ReadProperty<bool>(201, "unknown");
	data.scale = deserializer.ReadProperty<uint8_t>(202, "scale");
	if (data.has_min_max) {
		data.min = deserializer.ReadProperty<Value>(203, "min");
		data.max = deserializer.ReadProperty<Value>(204, "max");
	}
}

child_list_t<Value> UnboundedNumericStats::ToStruct(const BaseStatistics &stats) {
	auto &data = GetData(stats);
	child_list_t<Value> result;
	if (data.has_min_max) {
		result.emplace_back("min", data.min);
		result.emplace_back("max", data.max);
	} else {
		result.emplace_back("min", Value());
		result.emplace_back("max", Value());
	}
	result.emplace_back("scale", Value::UTINYINT(data.scale));
	if (data.unknown) {
		result.emplace_back("unknown", Value::BOOLEAN(true));
	}
	return result;
}

} // namespace duckdb
