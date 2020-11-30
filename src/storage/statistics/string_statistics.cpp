#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/common/serializer.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

StringStatistics::StringStatistics(LogicalType type) : BaseStatistics(type) {
	for (idx_t i = 0; i < MAX_STRING_MINMAX_SIZE; i++) {
		min[i] = 0xFF;
		max[i] = 0;
	}
	max_string_length = 0;
	has_unicode = false;
	has_overflow_strings = false;
}

unique_ptr<BaseStatistics> StringStatistics::Copy() {
	auto stats = make_unique<StringStatistics>(type);
	memcpy(stats->min, min, MAX_STRING_MINMAX_SIZE);
	memcpy(stats->max, max, MAX_STRING_MINMAX_SIZE);
	stats->has_unicode = has_unicode;
	stats->max_string_length = max_string_length;
	stats->max_string_length = max_string_length;
	stats->has_null = has_null;
	return move(stats);
}

void StringStatistics::Serialize(Serializer &serializer) {
	BaseStatistics::Serialize(serializer);
	serializer.WriteData(min, MAX_STRING_MINMAX_SIZE);
	serializer.WriteData(max, MAX_STRING_MINMAX_SIZE);
	serializer.Write<bool>(has_unicode);
	serializer.Write<uint32_t>(max_string_length);
	serializer.Write<bool>(has_overflow_strings);
}

unique_ptr<BaseStatistics> StringStatistics::Deserialize(Deserializer &source, LogicalType type) {
	auto stats = make_unique<StringStatistics>(type);
	source.ReadData(stats->min, MAX_STRING_MINMAX_SIZE);
	source.ReadData(stats->max, MAX_STRING_MINMAX_SIZE);
	stats->has_unicode = source.Read<bool>();
	stats->max_string_length = source.Read<uint32_t>();
	stats->has_overflow_strings = source.Read<bool>();
	return move(stats);
}

static int string_value_comparison(const_data_ptr_t data, idx_t len, const_data_ptr_t comparison) {
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

void StringStatistics::Update(const string_t &value) {
	auto data = (const_data_ptr_t)value.GetDataUnsafe();
	auto size = value.GetSize();

	//! we can only fit 8 bytes, so we might need to trim our string
	idx_t value_size = size > MAX_STRING_MINMAX_SIZE ? MAX_STRING_MINMAX_SIZE : size;
	// update the min and max
	if (string_value_comparison(data, value_size, min) < 0) {
		memcpy(min, data, value_size);
		for (idx_t i = value_size; i < MAX_STRING_MINMAX_SIZE; i++) {
			min[i] = '\0';
		}
	}
	if (string_value_comparison(data, value_size, max) > 0) {
		memcpy(max, data, value_size);
		for (idx_t i = value_size; i < MAX_STRING_MINMAX_SIZE; i++) {
			max[i] = '\0';
		}
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

void StringStatistics::Merge(const BaseStatistics &other_) {
	auto &other = (const StringStatistics &)other_;
	if (string_value_comparison(other.min, MAX_STRING_MINMAX_SIZE, min) < 0) {
		memcpy(min, other.min, MAX_STRING_MINMAX_SIZE);
	}
	if (string_value_comparison(other.max, MAX_STRING_MINMAX_SIZE, max) > 0) {
		memcpy(max, other.max, MAX_STRING_MINMAX_SIZE);
	}
	has_null = has_null || other.has_null;
	has_unicode = has_unicode || other.has_unicode;
	max_string_length = MaxValue<uint32_t>(max_string_length, other.max_string_length);
	has_overflow_strings = has_overflow_strings || other.has_overflow_strings;
}

bool StringStatistics::CheckZonemap(ExpressionType comparison_type, string constant) {
	auto data = (const_data_ptr_t)constant.c_str();
	auto size = constant.size();

	idx_t value_size = size > MAX_STRING_MINMAX_SIZE ? MAX_STRING_MINMAX_SIZE : size;
	int min_comp = string_value_comparison(data, value_size, min);
	int max_comp = string_value_comparison(data, value_size, max);
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return min_comp >= 0 && max_comp <= 0;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
		return max_comp <= 0;
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return min_comp >= 0;
	default:
		throw InternalException("Operation not implemented");
	}
}

string StringStatistics::ToString() {
	idx_t min_len, max_len;
	for (min_len = 0; min_len < MAX_STRING_MINMAX_SIZE; min_len++) {
		if (min[min_len] == '\0') {
			break;
		}
	}
	for (max_len = 0; max_len < MAX_STRING_MINMAX_SIZE; max_len++) {
		if (max[max_len] == '\0') {
			break;
		}
	}
	return StringUtil::Format(
	    "String Statistics [Has Null: %s, Min: %s, Max: %s, Has Unicode: %s, Max String Length: %lld]",
	    has_null ? "true" : "false", string((const char *)min, min_len), string((const char *)max, max_len),
	    has_unicode ? "true" : "false", max_string_length);
}

void StringStatistics::Verify(Vector &vector, idx_t count) {
	BaseStatistics::Verify(vector, count);

	string_t min_string((const char *)min, MAX_STRING_MINMAX_SIZE);
	string_t max_string((const char *)max, MAX_STRING_MINMAX_SIZE);

	VectorData vdata;
	vector.Orrify(count, vdata);
	auto data = (string_t *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto index = vdata.sel->get_index(i);
		if ((*vdata.nullmask)[index]) {
			continue;
		}
		auto value = data[index];
		auto data = value.GetDataUnsafe();
		auto len = value.GetSize();
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
		if (string_value_comparison((const_data_ptr_t) data, MinValue<idx_t>(len, MAX_STRING_MINMAX_SIZE), min) < 0) {
			throw InternalException("Statistics mismatch: value is smaller than min.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		}
		if (string_value_comparison((const_data_ptr_t) data, MinValue<idx_t>(len, MAX_STRING_MINMAX_SIZE), max) > 0) {
			throw InternalException("Statistics mismatch: value is bigger than max.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		}
	}
}

} // namespace duckdb
