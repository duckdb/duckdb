#include "duckdb/storage/statistics/string_stats.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/storage/statistics/string_stats_writer.hpp"

namespace duckdb {

BaseStatistics StringStats::CreateUnknown(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	auto &string_data = StringStats::GetDataUnsafe(result);
	string_data.max_string_length = 0;
	string_data.has_max_string_length = false;
	string_data.has_unicode = true;
	string_data.min_type = StringStatsType::NO_STATS;
	string_data.max_type = StringStatsType::NO_STATS;
	return result;
}

BaseStatistics StringStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	auto &string_data = StringStats::GetDataUnsafe(result);
	string_data.max_string_length = 0;
	string_data.has_max_string_length = true;
	string_data.has_unicode = false;
	string_data.min_type = StringStatsType::EMPTY_STATS;
	string_data.max_type = StringStatsType::EMPTY_STATS;
	return result;
}

StringStatsData &StringStats::GetDataUnsafe(BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::STRING_STATS);
	return stats.stats_union.string_data;
}

const StringStatsData &StringStats::GetDataUnsafe(const BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::STRING_STATS);
	return stats.stats_union.string_data;
}

bool StatsIsSet(StringStatsType type) {
	return type == StringStatsType::TRUNCATED_STATS || type == StringStatsType::EXACT_STATS;
}

bool StringStats::HasMinMax(const BaseStatistics &stats) {
	if (stats.GetType().id() == LogicalTypeId::SQLNULL) {
		return false;
	}
	auto &string_data = StringStats::GetDataUnsafe(stats);
	return StatsIsSet(string_data.min_type) && StatsIsSet(string_data.max_type);
}

bool StringStats::HasMin(const BaseStatistics &stats) {
	return StatsIsSet(GetMinType(stats));
}

bool StringStats::HasMax(const BaseStatistics &stats) {
	return StatsIsSet(GetMaxType(stats));
}

StringStatsType StringStats::GetMinType(const BaseStatistics &stats) {
	if (stats.GetType().id() == LogicalTypeId::SQLNULL) {
		return StringStatsType::NO_STATS;
	}
	auto &string_data = StringStats::GetDataUnsafe(stats);
	return string_data.min_type;
}

StringStatsType StringStats::GetMaxType(const BaseStatistics &stats) {
	if (stats.GetType().id() == LogicalTypeId::SQLNULL) {
		return StringStatsType::NO_STATS;
	}
	auto &string_data = StringStats::GetDataUnsafe(stats);
	return string_data.max_type;
}

bool StringStats::HasMaxStringLength(const BaseStatistics &stats) {
	if (stats.GetType().id() == LogicalTypeId::SQLNULL) {
		return false;
	}
	return StringStats::GetDataUnsafe(stats).has_max_string_length;
}

uint32_t StringStats::MaxStringLength(const BaseStatistics &stats) {
	if (!HasMaxStringLength(stats)) {
		throw InternalException("MaxStringLength called on statistics that does not have a max string length");
	}
	return StringStats::GetDataUnsafe(stats).max_string_length;
}

bool StringStats::CanContainUnicode(const BaseStatistics &stats) {
	if (stats.GetType().id() == LogicalTypeId::SQLNULL) {
		return true;
	}
	return StringStats::GetDataUnsafe(stats).has_unicode;
}

string GetStringMinMaxValue(const data_t data[]) {
	idx_t len;
	for (len = 0; len < StringStatsData::MAX_STRING_MINMAX_SIZE; len++) {
		if (!data[len]) {
			break;
		}
	}
	return string(const_char_ptr_cast(data), len);
}

string StringStats::Min(const BaseStatistics &stats) {
	auto &string_data = GetDataUnsafe(stats);
	if (!StatsIsSet(string_data.min_type)) {
		throw InternalException("StringStats::Min called but no string stats were found - call StringStats::HasMin or "
		                        "StringStats::HasMinMax first");
	}
	return string_data.min.GetString();
}

string StringStats::Max(const BaseStatistics &stats) {
	auto &string_data = GetDataUnsafe(stats);
	if (!StatsIsSet(string_data.max_type)) {
		throw InternalException("StringStats::Max called but no string stats were found - call StringStats::HasMax or "
		                        "StringStats::HasMinMax first");
	}
	return string_data.max.GetString();
}

void StringStats::ResetMaxStringLength(BaseStatistics &stats) {
	GetDataUnsafe(stats).has_max_string_length = false;
}

void StringStats::SetMaxStringLength(BaseStatistics &stats, uint32_t length) {
	auto &data = GetDataUnsafe(stats);
	data.has_max_string_length = true;
	data.max_string_length = length;
}

void StringStats::SetContainsUnicode(BaseStatistics &stats) {
	GetDataUnsafe(stats).has_unicode = true;
}

void LegacyConstructMinMax(string_t input, StringStatsType type, data_t result[], bool is_min) {
	auto input_data = const_data_ptr_cast(input.GetData());
	if (type == StringStatsType::EMPTY_STATS) {
		// for empty stats we serialize min as the maximum value, and max as the minimum value
		data_t empty_byte = is_min ? 0xFF : 0x00;
		memset(result, empty_byte, StringStatsData::MAX_STRING_MINMAX_SIZE);
	} else if (type == StringStatsType::NO_STATS) {
		// for no stats we serialize min as 0x00..., and max as 0xFF...
		data_t no_stats_byte = is_min ? 0x00 : 0xFF;
		memset(result, no_stats_byte, StringStatsData::MAX_STRING_MINMAX_SIZE);
	} else {
		StringStatsWriter::ConstructValue(input_data, input.GetSize(), result);
	}
}

string_t LegacyReadMinMax(const data_t result[]) {
	// truncate any trailing 0-bytes - there's no reason to keep them around
	uint32_t len = StringStatsData::MAX_STRING_MINMAX_SIZE;
	for (; len > 0; len--) {
		if (result[len - 1] != '\0') {
			break;
		}
	}
	return string_t(const_char_ptr_cast(result), len);
}

StringStatsType LegacyGetMinMaxType(const data_t min[], const data_t max[], bool has_max_length, idx_t max_length) {
	if (min[0] > max[0]) {
		// if min > max then we have empty stats
		return StringStatsType::EMPTY_STATS;
	}
	if (StringStatsWriter::AllCharsEqualTo(min, 0x00) && StringStatsWriter::AllCharsEqualTo(max, 0xFF)) {
		// if min is 0x00... and max is 0xFF... then we have no min/max (nothing can ever be pruned)
		return StringStatsType::NO_STATS;
	}
	if (has_max_length && max_length <= StringStatsData::MAX_STRING_MINMAX_SIZE) {
		// when:
		// (1) max length is known, and
		// (2) max length is <= 8, and
		// (3) min/max are equivalent to the max length
		// We know the min/max are exact because they have not been truncated
		// if min/max are not equal to the max length - we can't distinguish between e.g. `hello\0` and `hello`
		if (max_length == 0 || (min[max_length - 1] != '\0' && max[max_length - 1] != '\0')) {
			return StringStatsType::EXACT_STATS;
		}
	}
	return StringStatsType::TRUNCATED_STATS;
}

void StringStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &string_data = GetDataUnsafe(stats);
	data_t min_data[StringStatsData::MAX_STRING_MINMAX_SIZE];
	data_t max_data[StringStatsData::MAX_STRING_MINMAX_SIZE];
	LegacyConstructMinMax(string_data.min, string_data.min_type, min_data, true);
	LegacyConstructMinMax(string_data.max, string_data.max_type, max_data, false);
	serializer.WriteProperty(200, "min", min_data, StringStatsData::MAX_STRING_MINMAX_SIZE);
	serializer.WriteProperty(201, "max", max_data, StringStatsData::MAX_STRING_MINMAX_SIZE);
	serializer.WriteProperty(202, "has_unicode", string_data.has_unicode);
	serializer.WriteProperty(203, "has_max_string_length", string_data.has_max_string_length);
	serializer.WriteProperty(204, "max_string_length", string_data.max_string_length);
}

void StringStats::Deserialize(Deserializer &deserializer, BaseStatistics &base) {
	auto &string_data = GetDataUnsafe(base);
	data_t min_data[StringStatsData::MAX_STRING_MINMAX_SIZE];
	data_t max_data[StringStatsData::MAX_STRING_MINMAX_SIZE];
	deserializer.ReadProperty(200, "min", min_data, StringStatsData::MAX_STRING_MINMAX_SIZE);
	deserializer.ReadProperty(201, "max", max_data, StringStatsData::MAX_STRING_MINMAX_SIZE);
	deserializer.ReadProperty(202, "has_unicode", string_data.has_unicode);
	deserializer.ReadProperty(203, "has_max_string_length", string_data.has_max_string_length);
	deserializer.ReadProperty(204, "max_string_length", string_data.max_string_length);
	string_data.min = AssignString(base, LegacyReadMinMax(min_data), true);
	string_data.max = AssignString(base, LegacyReadMinMax(max_data), false);
	string_data.min_type =
	    LegacyGetMinMaxType(min_data, max_data, string_data.has_max_string_length, string_data.max_string_length);
	string_data.max_type = string_data.min_type;
}

void StringStats::Update(BaseStatistics &stats, const string_t &value) {
	//! we can only fit 8 bytes, so we might need to trim our string
	// FIXME: allow larger strings instead of falling back to the
	StringStatsWriter writer(stats.GetType());
	writer.Update(value);
	writer.Merge(stats);
}

struct StringData {
	unique_ptr<data_t[]> data;
	idx_t capacity = 0;

	string_t AssignString(const string_t &value) {
		if (value.GetSize() > capacity) {
			auto next_capacity = NextPowerOfTwo(value.GetSize());
			data = make_uniq_array<data_t>(next_capacity);
			capacity = next_capacity;
		}
		memcpy(data.get(), value.GetData(), value.GetSize());
		return string_t(const_char_ptr_cast(data.get()), static_cast<uint32_t>(value.GetSize()));
	}
};

struct StringStatsExtraData : public ExtraStatsData {
	StringData string_data[2];
};

string_t StringStats::AssignString(BaseStatistics &stats, const string_t &input, bool is_min) {
	if (input.IsInlined()) {
		return input;
	}
	if (!stats.extra_data) {
		stats.extra_data = make_uniq<StringStatsExtraData>();
	}
	auto &extra_data = stats.extra_data->Cast<StringStatsExtraData>();
	auto data_idx = is_min ? 0ULL : 1ULL;
	return extra_data.string_data[data_idx].AssignString(input);
}

void StringStats::SetMin(BaseStatistics &stats, const string_t &value, StringStatsType type) {
	auto &stats_data = GetDataUnsafe(stats);
	stats_data.min = AssignString(stats, value, true);
	stats_data.min_type = type;
}

void StringStats::SetMax(BaseStatistics &stats, const string_t &value, StringStatsType type) {
	auto &stats_data = GetDataUnsafe(stats);
	stats_data.max = AssignString(stats, value, false);
	stats_data.max_type = type;
}

void StringStats::SetMin(BaseStatistics &stats, const string_t &value) {
	SetMin(stats, value, StringStatsType::TRUNCATED_STATS);
}

void StringStats::SetMax(BaseStatistics &stats, const string_t &value) {
	SetMax(stats, value, StringStatsType::TRUNCATED_STATS);
}

void StringStats::Copy(BaseStatistics &stats, const BaseStatistics &other) {
	auto &string_data = GetDataUnsafe(stats);
	auto &other_data = GetDataUnsafe(other);
	if (StatsIsSet(other_data.min_type)) {
		string_data.min = AssignString(stats, other_data.min, true);
	}
	if (StatsIsSet(other_data.max_type)) {
		string_data.max = AssignString(stats, other_data.max, false);
	}
}

void StringStats::MergeStats(BaseStatistics &stats, string_t &target, StringStatsType &target_type,
                             const string_t &source, StringStatsType source_type, bool is_min) {
	if (target_type == StringStatsType::NO_STATS || source_type == StringStatsType::NO_STATS) {
		// no min/max available - result is no min/max
		target_type = StringStatsType::NO_STATS;
		return;
	}
	if (source_type == StringStatsType::EMPTY_STATS) {
		// source is empty - nothing to update
		return;
	}
	if (target_type == StringStatsType::EMPTY_STATS) {
		// we don't have min/max - copy them from the target
		target = AssignString(stats, source, is_min);
		target_type = source_type;
		return;
	}
	// both min/max stats are there - compare
	bool new_is_more_extreme;
	if (is_min) {
		new_is_more_extreme = LessThan::Operation(source, target);
	} else {
		new_is_more_extreme = GreaterThan::Operation(source, target);
	}
	if (!new_is_more_extreme) {
		// old value is more extreme - bail
		return;
	}
	// assign the new value
	target = AssignString(stats, source, is_min);
	target_type = source_type;
}

void StringStats::Merge(BaseStatistics &stats, const StringStatsData &other_data) {
	auto &string_data = GetDataUnsafe(stats);

	// merge min/max
	MergeStats(stats, string_data.min, string_data.min_type, other_data.min, other_data.min_type, true);
	MergeStats(stats, string_data.max, string_data.max_type, other_data.max, other_data.max_type, false);
	string_data.has_unicode = string_data.has_unicode || other_data.has_unicode;
	string_data.has_max_string_length = string_data.has_max_string_length && other_data.has_max_string_length;
	string_data.max_string_length = MaxValue<uint32_t>(string_data.max_string_length, other_data.max_string_length);
}

void StringStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}
	if (other.GetType().id() == LogicalTypeId::SQLNULL) {
		return;
	}
	auto &other_data = GetDataUnsafe(other);
	Merge(stats, other_data);
}

void StringStats::Merge(BaseStatistics &stats, const StringStatsWriter &stats_writer) {
	if (!stats_writer.HasStats()) {
		return;
	}
	// construct string stats data from the writer
	StringStatsData other_data;
	other_data.min = LegacyReadMinMax(stats_writer.min);
	other_data.max = LegacyReadMinMax(stats_writer.max);
	other_data.min_type = LegacyGetMinMaxType(stats_writer.min, stats_writer.max, true, stats_writer.max_string_length);
	other_data.max_type = other_data.min_type;
	other_data.has_unicode = stats_writer.has_unicode;
	other_data.has_max_string_length = true;
	other_data.max_string_length = stats_writer.max_string_length;
	Merge(stats, other_data);
}

FilterPropagateResult StringStats::CheckZonemap(const BaseStatistics &stats, ExpressionType comparison_type,
                                                array_ptr<const Value> constants) {
	auto &string_data = GetDataUnsafe(stats);
	D_ASSERT(stats.CanHaveNoNull());
	for (auto &constant_value : constants) {
		D_ASSERT(constant_value.type() == stats.GetType());
		D_ASSERT(!constant_value.IsNull());
		auto &constant = StringValue::Get(constant_value);
		FilterPropagateResult prune_result;
		if (HasMinMax(stats)) {
			prune_result = CheckZonemap(string_data.min, string_data.min_type, string_data.max, string_data.max_type,
			                            comparison_type, constant);
		} else {
			prune_result = FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		if (prune_result == FilterPropagateResult::NO_PRUNING_POSSIBLE) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else if (prune_result == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
	}
	return FilterPropagateResult::FILTER_ALWAYS_FALSE;
}

int8_t CompareStringStats(string_t input, string_t stats, StringStatsType type) {
	if (type == StringStatsType::TRUNCATED_STATS && input.GetSize() > stats.GetSize()) {
		// if the stats are truncated we can only compare at most the bytes as are present in the stats
		return Comparator::Operation(string_t(input.GetData(), static_cast<uint32_t>(stats.GetSize())), stats);
	}
	return Comparator::Operation(input, stats);
}

FilterPropagateResult StringStats::CheckZonemap(string_t min, StringStatsType min_type, string_t max,
                                                StringStatsType max_type, ExpressionType comparison_type,
                                                string_t constant) {
	auto min_comp = CompareStringStats(constant, min, min_type);
	auto max_comp = CompareStringStats(constant, max, max_type);
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		if (min_comp >= 0 && max_comp <= 0) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_DISTINCT_FROM:
		if (min_comp < 0 || max_comp > 0) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
		if (max_comp <= 0) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		if (min_comp >= 0) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	default:
		throw InternalException("Expression type not implemented for string statistics zone map");
	}
}

child_list_t<Value> StringStats::ToStruct(const BaseStatistics &stats) {
	child_list_t<Value> result;
	auto &string_data = GetDataUnsafe(stats);
	if (HasMinMax(stats)) {
		result.emplace_back("min", Blob::ToString(string_data.min));
		result.emplace_back("max", Blob::ToString(string_data.max));
	}
	result.emplace_back("has_unicode", Value::BOOLEAN(string_data.has_unicode));
	if (HasMaxStringLength(stats)) {
		result.emplace_back("max_string_length", Value::UBIGINT(string_data.max_string_length));
	}
	return result;
}

void StringStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	auto &string_data = GetDataUnsafe(stats);

	auto entries = vector.Values<string_t>();
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto entry = entries[idx];
		if (!entry.IsValid()) {
			continue;
		}
		auto value = entry.GetValue();
		auto data = value.GetData();
		auto len = value.GetSize();
		// LCOV_EXCL_START
		if (string_data.has_max_string_length && len > string_data.max_string_length) {
			throw InternalException(
			    "Statistics mismatch: string value exceeds maximum string length.\nStatistics: %s\nVector: %s",
			    stats.ToString(), vector.ToString());
		}
		if (stats.GetType().id() == LogicalTypeId::VARCHAR && !string_data.has_unicode) {
			auto unicode = Utf8Proc::Analyze(data, len);
			if (unicode == UnicodeType::UTF8) {
				throw InternalException("Statistics mismatch: string value contains unicode, but statistics says it "
				                        "shouldn't.\nStatistics: %s\nVector: %s",
				                        stats.ToString(), vector.ToString());
			} else if (unicode == UnicodeType::INVALID) {
				throw InternalException("Invalid unicode detected in vector: %s", vector.ToString());
			}
		}
		auto min_cmp = CompareStringStats(value, string_data.min, string_data.min_type);
		if (min_cmp < 0) {
			throw InternalException("Statistics mismatch: value is smaller than min.\nStatistics: %s\nVector: %s",
			                        stats.ToString(), vector.ToString());
		}
		auto max_cmp = CompareStringStats(value, string_data.max, string_data.max_type);
		if (max_cmp > 0) {
			throw InternalException("Statistics mismatch: value is bigger than max.\nStatistics: %s\nVector: %s",
			                        stats.ToString(), vector.ToString());
		}
		// LCOV_EXCL_STOP
	}
}

} // namespace duckdb
