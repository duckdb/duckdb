#include "duckdb/storage/compression/dict_fsst/analyze.hpp"

namespace duckdb {
namespace dict_fsst {

//! Determine the size requirements for the worst case, which is when a single string fills an
//! entire segment on its own.
static idx_t RequiredSpace(const idx_t str_len, const bool fsst_encoded) {
	// Dictionary contains NULL and current string.
	const bitpacking_width_t string_lengths_width = BitpackingPrimitives::MinimumBitWidth(str_len);
	const idx_t string_lengths_space = BitpackingPrimitives::GetRequiredSize(2, string_lengths_width);

	// Dictionary stores only one valid string.
	const bitpacking_width_t dict_indices_width = BitpackingPrimitives::MinimumBitWidth(1);
	const idx_t dict_indices_space = BitpackingPrimitives::GetRequiredSize(1, dict_indices_width);

	idx_t size = 0;
	size += sizeof(dict_fsst_compression_header_t);
	size = AlignValue<idx_t>(size);
	size += str_len;
	if (fsst_encoded) {
		// As denoted in fsst.h
		size += 7;
	}
	size = AlignValue<idx_t>(size);
	if (fsst_encoded) {
		size += DictFSSTCompression::FSST_SYMBOL_TABLE_SIZE;
		size = AlignValue<idx_t>(size);
	}
	size += string_lengths_space;
	size = AlignValue<idx_t>(size);
	size += dict_indices_space;

	return size;
}

//! Perform a binary search to find the effective exclusive string size limit.
static idx_t GetStringSizeLimit(const idx_t available_space, const bool fsst_encoded) {
	idx_t lower = 0;
	idx_t upper = DictFSSTCompression::STRING_SIZE_LIMIT;

	while (lower + 1 < upper) {
		const auto str_len = lower + (upper - lower) / 2;
		const auto encoded_len = fsst_encoded ? str_len * 2 : str_len;

		if (RequiredSpace(encoded_len, fsst_encoded) <= available_space) {
			lower = str_len;
		} else {
			upper = str_len;
		}
	}
	return upper;
}

DictFSSTAnalyzeState::DictFSSTAnalyzeState(const CompressionInfo &info) : AnalyzeState(info) {
	const auto block_size = info.GetBlockSize();

	string_size_limit = GetStringSizeLimit(block_size, false);
	fsst_string_size_limit = GetStringSizeLimit(block_size, true);
}

bool DictFSSTAnalyzeState::Analyze(Vector &input, idx_t count) {
	UnifiedVectorFormat vector_format;
	input.ToUnifiedFormat(count, vector_format);
	const auto strings = vector_format.GetData<string_t>(vector_format);

	for (idx_t i = 0; i < count; i++) {
		const auto idx = vector_format.sel->get_index(i);
		if (!vector_format.validity.RowIsValid(idx)) {
			contains_nulls = true;
		} else {
			const auto &str = strings[idx];
			const auto str_len = str.GetSize();
			total_string_length += str_len;
			if (str_len > max_string_length) {
				max_string_length = str_len;
			}
			if (str_len >= string_size_limit) {
				//! A segment cannot be spread out over multiple blocks, so if a string cannot fit in an empty segment
				//! the encoding will fail.
				return false;
			}
			if (str_len >= fsst_string_size_limit) {
				//! In the worst case FSST may double the string length by prepending every byte with an exception.
				disable_fsst = true;
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
