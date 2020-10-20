#include "duckdb/storage/table/string_statistics.hpp"
#include "duckdb/common/serializer.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

StringStatistics::StringStatistics() {
	for(idx_t i = 0; i < MAX_STRING_MINMAX_SIZE; i++) {
		min[i] = 0xFF;
		max[i] = 0;
	}
	max_string_length = 0;
	has_unicode = false;
	has_overflow_strings = false;
}

unique_ptr<BaseStatistics> StringStatistics::Copy() {
	auto stats = make_unique<StringStatistics>();
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

unique_ptr<BaseStatistics> StringStatistics::Deserialize(Deserializer &source) {
	auto stats = make_unique<StringStatistics>();
	source.ReadData(stats->min, MAX_STRING_MINMAX_SIZE);
	source.ReadData(stats->max, MAX_STRING_MINMAX_SIZE);
	stats->has_unicode = source.Read<bool>();
	stats->max_string_length = source.Read<uint32_t>();
	stats->has_overflow_strings = source.Read<bool>();
	return move(stats);
}

static int string_value_comparison(const_data_ptr_t data, idx_t len, data_ptr_t comparison) {
	assert(len <= StringStatistics::MAX_STRING_MINMAX_SIZE);
	for(idx_t i = 0; i < len; i++) {
		if (data[i] < comparison[i]) {
			return -1;
		} else if (data[i] > comparison[i]) {
			return 1;
		}
	}
	return 0;
}

void StringStatistics::Update(const string_t &value) {
	auto data = (const_data_ptr_t) value.GetData();
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
	// if (!has_unicode) {
	// 	auto unicode = Utf8Proc::Analyze((const char*) data, size);
	// 	if (unicode == UnicodeType::UNICODE) {
	// 		has_unicode = true;
	// 	} else if (unicode == UnicodeType::INVALID) {
	// 		throw InternalException("Invalid unicode detected in segment statistics update!");
	// 	}
	// }
}

bool StringStatistics::CheckZonemap(ExpressionType comparison_type, string constant) {
	auto data = (const_data_ptr_t) constant.c_str();
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

}
