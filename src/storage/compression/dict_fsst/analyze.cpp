#include "duckdb/storage/compression/dict_fsst/analyze.hpp"

namespace duckdb {
namespace dict_fsst {

DictFSSTAnalyzeState::DictFSSTAnalyzeState(const CompressionInfo &info) : AnalyzeState(info) {
}

bool DictFSSTAnalyzeState::Analyze(Vector &input, idx_t count) {
	for (auto entry : input.Values<string_t>(count)) {
		if (!entry.IsValid()) {
			contains_nulls = true;
			continue;
		}
		auto &str = entry.GetValue();
		auto str_len = str.GetSize();
		total_string_length += str_len;
		if (str_len > max_string_length) {
			max_string_length = str_len;
		}
		if (str_len >= DictFSSTCompression::STRING_SIZE_LIMIT) {
			//! This string is too long, we don't want to use DICT_FSST for this rowgroup
			return false;
		}
	}
	total_count += count;
	return true;
}

idx_t DictFSSTAnalyzeState::FinalAnalyze() {
	return LossyNumericCast<idx_t>((double)total_string_length / 2.0);
}

} // namespace dict_fsst
} // namespace duckdb
