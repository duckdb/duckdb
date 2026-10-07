#include "duckdb/function/scalar/string_common.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "utf8proc.hpp"

namespace duckdb {

namespace {

// Find the byte length of the first character_count UTF-8 code points.
bool GetPrefixByteLength(const string &value, idx_t character_count, idx_t &byte_count) {
	auto data = reinterpret_cast<const utf8proc_uint8_t *>(value.c_str());
	byte_count = 0;
	for (idx_t character_idx = 0; character_idx < character_count; character_idx++) {
		if (byte_count >= value.size()) {
			return false;
		}
		utf8proc_int32_t codepoint;
		auto codepoint_size =
		    utf8proc_iterate(data + byte_count, NumericCast<utf8proc_ssize_t>(value.size() - byte_count), &codepoint);
		if (codepoint_size <= 0) {
			return false;
		}
		byte_count += NumericCast<idx_t>(codepoint_size);
	}
	return true;
}

// Return at most character_count UTF-8 code points, preserving shorter values in full.
bool GetPrefix(const string &value, idx_t character_count, string &result) {
	idx_t prefix_size;
	if (!GetPrefixByteLength(value, character_count, prefix_size) && prefix_size != value.size()) {
		return false;
	}
	result = value.substr(0, prefix_size);
	return true;
}

bool GetStringSliceStatsBound(const string &value, idx_t prefix_size, optional_idx character_count, string &result) {
	auto suffix = value.substr(prefix_size);
	if (!character_count.IsValid()) {
		result = std::move(suffix);
		return true;
	}
	return GetPrefix(suffix, character_count.GetIndex(), result);
}

} // namespace

unique_ptr<BaseStatistics> PropagateStringSliceStats(FunctionStatisticsInput &input, idx_t start_character_index,
                                                     optional_idx character_count) {
	auto &string_stats = input.child_stats[0];
	if (!StringStats::HasMinMax(string_stats)) {
		return nullptr;
	}

	auto min = StringStats::Min(string_stats);
	auto max = StringStats::Max(string_stats);
	idx_t prefix_size = 0;
	if (!GetPrefixByteLength(min, start_character_index, prefix_size) ||
	    prefix_size > StringUtil::GetCommonPrefixSize(min, max)) {
		return nullptr;
	}

	string result_min;
	string result_max;
	if (!GetStringSliceStatsBound(min, prefix_size, character_count, result_min) ||
	    !GetStringSliceStatsBound(max, prefix_size, character_count, result_max)) {
		return nullptr;
	}

	auto result = StringStats::CreateUnknown(input.expr.GetReturnType());
	StringStats::SetMin(result, string_t(result_min), StringStats::GetMinType(string_stats));
	StringStats::SetMax(result, string_t(result_max), StringStats::GetMaxType(string_stats));
	result.CopyValidity(string_stats);
	return result.ToUnique();
}

} // namespace duckdb
