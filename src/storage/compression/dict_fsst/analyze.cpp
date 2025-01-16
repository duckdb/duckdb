#include "duckdb/storage/compression/dict_fsst/analyze.hpp"
#include "fsst.h"

namespace duckdb {
namespace dict_fsst {

DictFSSTAnalyzeState::DictFSSTAnalyzeState(const CompressionInfo &info) : DictFSSTCompressionState(info) {
}

bool DictFSSTAnalyzeState::Analyze(Vector &input, idx_t count) {
	UnifiedVectorFormat vector_format;
	input.ToUnifiedFormat(count, vector_format);
	auto strings = vector_format.GetData<string_t>(vector_format);

	for (idx_t i = 0; i < count; i++) {
		auto idx = vector_format.sel->get_index(i);
		if (!vector_format.validity.RowIsValid(i)) {
			contains_nulls = true;
		} else {
			auto &str = strings[idx];
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
	}
	total_count += count;
	return true;
}

idx_t DictFSSTAnalyzeState::FinalAnalyze() {
	return LossyNumericCast<idx_t>((double)total_string_length / 2.0);
}

} // namespace dict_fsst
} // namespace duckdb
