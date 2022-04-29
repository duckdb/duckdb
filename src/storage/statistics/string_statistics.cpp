#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/common/field_writer.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

StringStatistics::StringStatistics(LogicalType type_p, StatisticsType stats_type)
    : BaseStatistics(move(type_p), stats_type) {
	InitializeBase();
	for (idx_t i = 0; i < MAX_STRING_MINMAX_SIZE; i++) {
		min[i] = 0xFF;
		max[i] = 0;
	}
	max_string_length = 0;
	has_unicode = false;
	has_overflow_strings = false;
}

unique_ptr<BaseStatistics> StringStatistics::Copy() const {
	auto result = make_unique<StringStatistics>(type, stats_type);
	result->CopyBase(*this);

	memcpy(result->min, min, MAX_STRING_MINMAX_SIZE);
	memcpy(result->max, max, MAX_STRING_MINMAX_SIZE);
	result->has_unicode = has_unicode;
	result->max_string_length = max_string_length;
	return move(result);
}

void StringStatistics::Serialize(FieldWriter &writer) const {
	writer.WriteBlob(min, MAX_STRING_MINMAX_SIZE);
	writer.WriteBlob(max, MAX_STRING_MINMAX_SIZE);
	writer.WriteField<bool>(has_unicode);
	writer.WriteField<uint32_t>(max_string_length);
	writer.WriteField<bool>(has_overflow_strings);
}

unique_ptr<BaseStatistics> StringStatistics::Deserialize(FieldReader &reader, LogicalType type) {
	auto stats = make_unique<StringStatistics>(move(type), StatisticsType::LOCAL_STATS);
	reader.ReadBlob(stats->min, MAX_STRING_MINMAX_SIZE);
	reader.ReadBlob(stats->max, MAX_STRING_MINMAX_SIZE);
	stats->has_unicode = reader.ReadRequired<bool>();
	stats->max_string_length = reader.ReadRequired<uint32_t>();
	stats->has_overflow_strings = reader.ReadRequired<bool>();
	return move(stats);
}

static int StringValueComparison(const_data_ptr_t data, idx_t len, const_data_ptr_t comparison) {
	D_ASSERT(len <= StringStatistics::MAX_STRING_MINMAX_SIZE);
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
	idx_t value_size =
	    size > StringStatistics::MAX_STRING_MINMAX_SIZE ? StringStatistics::MAX_STRING_MINMAX_SIZE : size;
	memcpy(target, data, value_size);
	for (idx_t i = value_size; i < StringStatistics::MAX_STRING_MINMAX_SIZE; i++) {
		target[i] = '\0';
	}
}

void StringStatistics::Update(const string_t &value) {
	auto data = (const_data_ptr_t)value.GetDataUnsafe();
	auto size = value.GetSize();

	//! we can only fit 8 bytes, so we might need to trim our string
	// construct the value
	data_t target[MAX_STRING_MINMAX_SIZE];
	ConstructValue(data, size, target);

	// update the min and max
	if (StringValueComparison(target, MAX_STRING_MINMAX_SIZE, min) < 0) {
		memcpy(min, target, MAX_STRING_MINMAX_SIZE);
	}
	if (StringValueComparison(target, MAX_STRING_MINMAX_SIZE, max) > 0) {
		memcpy(max, target, MAX_STRING_MINMAX_SIZE);
	}
	if (size > max_string_length) {
		max_string_length = size;
	}
	if (type.id() == LogicalTypeId::VARCHAR && !has_unicode) {
		auto unicode = Utf8Proc::Analyze((const char *)data, size);
		if (unicode == UnicodeType::UNICODE) {
			has_unicode = true;
		} else if (unicode == UnicodeType::INVALID) {
			throw InternalException("Invalid unicode detected in segment statistics update!");
		}
	}
}

void StringStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);
	auto &other = (const StringStatistics &)other_p;
	if (StringValueComparison(other.min, MAX_STRING_MINMAX_SIZE, min) < 0) {
		memcpy(min, other.min, MAX_STRING_MINMAX_SIZE);
	}
	if (StringValueComparison(other.max, MAX_STRING_MINMAX_SIZE, max) > 0) {
		memcpy(max, other.max, MAX_STRING_MINMAX_SIZE);
	}
	has_unicode = has_unicode || other.has_unicode;
	max_string_length = MaxValue<uint32_t>(max_string_length, other.max_string_length);
	has_overflow_strings = has_overflow_strings || other.has_overflow_strings;
}

FilterPropagateResult StringStatistics::CheckZonemap(ExpressionType comparison_type, const string &constant) const {
	auto data = (const_data_ptr_t)constant.c_str();
	auto size = constant.size();

	idx_t value_size = size > MAX_STRING_MINMAX_SIZE ? MAX_STRING_MINMAX_SIZE : size;
	int min_comp = StringValueComparison(data, value_size, min);
	int max_comp = StringValueComparison(data, value_size, max);
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
	for (idx_t i = 0; i < StringStatistics::MAX_STRING_MINMAX_SIZE; i++) {
		if (data[i] == '\0') {
			return i;
		}
		if ((data[i] & 0x80) != 0) {
			return i;
		}
	}
	return StringStatistics::MAX_STRING_MINMAX_SIZE;
}

string StringStatistics::ToString() const {
	idx_t min_len = GetValidMinMaxSubstring(min);
	idx_t max_len = GetValidMinMaxSubstring(max);
	return StringUtil::Format("[Min: %s, Max: %s, Has Unicode: %s, Max String Length: %lld]%s",
	                          string((const char *)min, min_len), string((const char *)max, max_len),
	                          has_unicode ? "true" : "false", max_string_length, BaseStatistics::ToString());
}

void StringStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	BaseStatistics::Verify(vector, sel, count);

	string_t min_string((const char *)min, MAX_STRING_MINMAX_SIZE);
	string_t max_string((const char *)max, MAX_STRING_MINMAX_SIZE);

	VectorData vdata;
	vector.Orrify(count, vdata);
	auto data = (string_t *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		if (!vdata.validity.RowIsValid(index)) {
			continue;
		}
		auto value = data[index];
		auto data = value.GetDataUnsafe();
		auto len = value.GetSize();
		// LCOV_EXCL_START
		if (len > max_string_length) {
			throw InternalException(
			    "Statistics mismatch: string value exceeds maximum string length.\nStatistics: %s\nVector: %s",
			    ToString(), vector.ToString(count));
		}
		if (type.id() == LogicalTypeId::VARCHAR && !has_unicode) {
			auto unicode = Utf8Proc::Analyze(data, len);
			if (unicode == UnicodeType::UNICODE) {
				throw InternalException("Statistics mismatch: string value contains unicode, but statistics says it "
				                        "shouldn't.\nStatistics: %s\nVector: %s",
				                        ToString(), vector.ToString(count));
			} else if (unicode == UnicodeType::INVALID) {
				throw InternalException("Invalid unicode detected in vector: %s", vector.ToString(count));
			}
		}
		if (StringValueComparison((const_data_ptr_t)data, MinValue<idx_t>(len, MAX_STRING_MINMAX_SIZE), min) < 0) {
			throw InternalException("Statistics mismatch: value is smaller than min.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		}
		if (StringValueComparison((const_data_ptr_t)data, MinValue<idx_t>(len, MAX_STRING_MINMAX_SIZE), max) > 0) {
			throw InternalException("Statistics mismatch: value is bigger than max.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		}
		// LCOV_EXCL_STOP
	}
}

} // namespace duckdb
