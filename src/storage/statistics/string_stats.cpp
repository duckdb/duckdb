#include "duckdb/storage/statistics/string_stats.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "utf8proc_wrapper.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

BaseStatistics StringStats::CreateUnknown(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	auto &string_data = StringStats::GetDataUnsafe(result);
	for (idx_t i = 0; i < StringStatsData::MAX_STRING_MINMAX_SIZE; i++) {
		string_data.min[i] = 0;
		string_data.max[i] = 0xFF;
	}
	string_data.max_string_length = 0;
	string_data.has_max_string_length = false;
	string_data.has_unicode = true;
	return result;
}

BaseStatistics StringStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	auto &string_data = StringStats::GetDataUnsafe(result);
	for (idx_t i = 0; i < StringStatsData::MAX_STRING_MINMAX_SIZE; i++) {
		string_data.min[i] = 0xFF;
		string_data.max[i] = 0;
	}
	string_data.max_string_length = 0;
	string_data.has_max_string_length = true;
	string_data.has_unicode = false;
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
	return GetStringMinMaxValue(StringStats::GetDataUnsafe(stats).min);
}

string StringStats::Max(const BaseStatistics &stats) {
	return GetStringMinMaxValue(StringStats::GetDataUnsafe(stats).max);
}

void StringStats::ResetMaxStringLength(BaseStatistics &stats) {
	StringStats::GetDataUnsafe(stats).has_max_string_length = false;
}

void StringStats::SetContainsUnicode(BaseStatistics &stats) {
	StringStats::GetDataUnsafe(stats).has_unicode = true;
}

void StringStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &string_data = StringStats::GetDataUnsafe(stats);
	serializer.WriteProperty(200, "min", string_data.min, StringStatsData::MAX_STRING_MINMAX_SIZE);
	serializer.WriteProperty(201, "max", string_data.max, StringStatsData::MAX_STRING_MINMAX_SIZE);
	serializer.WriteProperty(202, "has_unicode", string_data.has_unicode);
	serializer.WriteProperty(203, "has_max_string_length", string_data.has_max_string_length);
	serializer.WriteProperty(204, "max_string_length", string_data.max_string_length);
}

void StringStats::Deserialize(Deserializer &deserializer, BaseStatistics &base) {
	auto &string_data = StringStats::GetDataUnsafe(base);
	deserializer.ReadProperty(200, "min", string_data.min, StringStatsData::MAX_STRING_MINMAX_SIZE);
	deserializer.ReadProperty(201, "max", string_data.max, StringStatsData::MAX_STRING_MINMAX_SIZE);
	deserializer.ReadProperty(202, "has_unicode", string_data.has_unicode);
	deserializer.ReadProperty(203, "has_max_string_length", string_data.has_max_string_length);
	deserializer.ReadProperty(204, "max_string_length", string_data.max_string_length);
}

static int StringValueComparison(const_data_ptr_t data, idx_t len, const_data_ptr_t comparison) {
	D_ASSERT(len <= StringStatsData::MAX_STRING_MINMAX_SIZE);
	for (idx_t i = 0; i < len; i++) {
		if (data[i] < comparison[i]) {
			return -1;
		} else if (data[i] > comparison[i]) {
			return 1;
		}
	}
	return 0;
}

static void ConstructValue(const_data_ptr_t data, idx_t size, data_t target[]) {
	idx_t value_size = size > StringStatsData::MAX_STRING_MINMAX_SIZE ? StringStatsData::MAX_STRING_MINMAX_SIZE : size;
	memcpy(target, data, value_size);
	for (idx_t i = value_size; i < StringStatsData::MAX_STRING_MINMAX_SIZE; i++) {
		target[i] = '\0';
	}
}

void StringStats::Update(BaseStatistics &stats, const string_t &value) {
	auto data = const_data_ptr_cast(value.GetData());
	auto size = value.GetSize();

	//! we can only fit 8 bytes, so we might need to trim our string
	// construct the value
	data_t target[StringStatsData::MAX_STRING_MINMAX_SIZE];
	ConstructValue(data, size, target);

	// update the min and max
	auto &string_data = StringStats::GetDataUnsafe(stats);
	if (StringValueComparison(target, StringStatsData::MAX_STRING_MINMAX_SIZE, string_data.min) < 0) {
		memcpy(string_data.min, target, StringStatsData::MAX_STRING_MINMAX_SIZE);
	}
	if (StringValueComparison(target, StringStatsData::MAX_STRING_MINMAX_SIZE, string_data.max) > 0) {
		memcpy(string_data.max, target, StringStatsData::MAX_STRING_MINMAX_SIZE);
	}
	if (size > string_data.max_string_length) {
		string_data.max_string_length = UnsafeNumericCast<uint32_t>(size);
	}
	if (stats.GetType().id() == LogicalTypeId::VARCHAR && !string_data.has_unicode) {
		auto unicode = Utf8Proc::Analyze(const_char_ptr_cast(data), size);
		if (unicode == UnicodeType::UNICODE) {
			string_data.has_unicode = true;
		} else if (unicode == UnicodeType::INVALID) {
			throw ErrorManager::InvalidUnicodeError(string(const_char_ptr_cast(data), size),
			                                        "segment statistics update");
		}
	}
}

void StringStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}
	auto &string_data = StringStats::GetDataUnsafe(stats);
	auto &other_data = StringStats::GetDataUnsafe(other);
	if (StringValueComparison(other_data.min, StringStatsData::MAX_STRING_MINMAX_SIZE, string_data.min) < 0) {
		memcpy(string_data.min, other_data.min, StringStatsData::MAX_STRING_MINMAX_SIZE);
	}
	if (StringValueComparison(other_data.max, StringStatsData::MAX_STRING_MINMAX_SIZE, string_data.max) > 0) {
		memcpy(string_data.max, other_data.max, StringStatsData::MAX_STRING_MINMAX_SIZE);
	}
	string_data.has_unicode = string_data.has_unicode || other_data.has_unicode;
	string_data.has_max_string_length = string_data.has_max_string_length && other_data.has_max_string_length;
	string_data.max_string_length = MaxValue<uint32_t>(string_data.max_string_length, other_data.max_string_length);
}

FilterPropagateResult StringStats::CheckZonemap(const BaseStatistics &stats, ExpressionType comparison_type,
                                                const string &constant) {
	auto &string_data = StringStats::GetDataUnsafe(stats);
	auto data = const_data_ptr_cast(constant.c_str());
	auto size = constant.size();

	idx_t value_size = size > StringStatsData::MAX_STRING_MINMAX_SIZE ? StringStatsData::MAX_STRING_MINMAX_SIZE : size;
	int min_comp = StringValueComparison(data, value_size, string_data.min);
	int max_comp = StringValueComparison(data, value_size, string_data.max);
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		if (min_comp >= 0 && max_comp <= 0) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_NOTEQUAL:
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

static idx_t GetValidMinMaxSubstring(const_data_ptr_t data) {
	for (idx_t i = 0; i < StringStatsData::MAX_STRING_MINMAX_SIZE; i++) {
		if (data[i] == '\0') {
			return i;
		}
		if ((data[i] & 0x80) != 0) {
			return i;
		}
	}
	return StringStatsData::MAX_STRING_MINMAX_SIZE;
}

string StringStats::ToString(const BaseStatistics &stats) {
	auto &string_data = StringStats::GetDataUnsafe(stats);
	idx_t min_len = GetValidMinMaxSubstring(string_data.min);
	idx_t max_len = GetValidMinMaxSubstring(string_data.max);
	return StringUtil::Format("[Min: %s, Max: %s, Has Unicode: %s, Max String Length: %s]",
	                          string(const_char_ptr_cast(string_data.min), min_len),
	                          string(const_char_ptr_cast(string_data.max), max_len),
	                          string_data.has_unicode ? "true" : "false",
	                          string_data.has_max_string_length ? to_string(string_data.max_string_length) : "?");
}

void StringStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	auto &string_data = StringStats::GetDataUnsafe(stats);

	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		if (!vdata.validity.RowIsValid(index)) {
			continue;
		}
		auto value = data[index];
		auto data = value.GetData();
		auto len = value.GetSize();
		// LCOV_EXCL_START
		if (string_data.has_max_string_length && len > string_data.max_string_length) {
			throw InternalException(
			    "Statistics mismatch: string value exceeds maximum string length.\nStatistics: %s\nVector: %s",
			    stats.ToString(), vector.ToString(count));
		}
		if (stats.GetType().id() == LogicalTypeId::VARCHAR && !string_data.has_unicode) {
			auto unicode = Utf8Proc::Analyze(data, len);
			if (unicode == UnicodeType::UNICODE) {
				throw InternalException("Statistics mismatch: string value contains unicode, but statistics says it "
				                        "shouldn't.\nStatistics: %s\nVector: %s",
				                        stats.ToString(), vector.ToString(count));
			} else if (unicode == UnicodeType::INVALID) {
				throw InternalException("Invalid unicode detected in vector: %s", vector.ToString(count));
			}
		}
		if (StringValueComparison(const_data_ptr_cast(data),
		                          MinValue<idx_t>(len, StringStatsData::MAX_STRING_MINMAX_SIZE), string_data.min) < 0) {
			throw InternalException("Statistics mismatch: value is smaller than min.\nStatistics: %s\nVector: %s",
			                        stats.ToString(), vector.ToString(count));
		}
		if (StringValueComparison(const_data_ptr_cast(data),
		                          MinValue<idx_t>(len, StringStatsData::MAX_STRING_MINMAX_SIZE), string_data.max) > 0) {
			throw InternalException("Statistics mismatch: value is bigger than max.\nStatistics: %s\nVector: %s",
			                        stats.ToString(), vector.ToString(count));
		}
		// LCOV_EXCL_STOP
	}
}

} // namespace duckdb
