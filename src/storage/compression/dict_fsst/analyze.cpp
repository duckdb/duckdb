#include "duckdb/storage/compression/dict_fsst/analyze.hpp"

#include <fsst.h>

namespace duckdb {
namespace dict_fsst {

static constexpr uint16_t FSST_SYMBOL_TABLE_SIZE2 = sizeof(duckdb_fsst_decoder_t);

DictFSSTAnalyzeState::DictFSSTAnalyzeState(const CompressionInfo &info) : AnalyzeState(info) {
}

//! Determine the size requirements for the worst case, which is when a single string fills an
//! entire segment on its own.
idx_t RequiredSpace(const idx_t str_len, const bool fsst_encoded) {
	// Dictionary contains NULL and current string.
	const idx_t string_lengths_width = BitpackingPrimitives::MinimumBitWidth(str_len);
	const idx_t string_lengths_space = BitpackingPrimitives::GetRequiredSize(2, string_lengths_width);

	// Dictionary stores only one valid string.
	const idx_t dict_indices_width = BitpackingPrimitives::MinimumBitWidth(1);
	const idx_t dict_indices_space = BitpackingPrimitives::GetRequiredSize(1, dict_indices_width);

	idx_t size = 0;
	size += sizeof(dict_fsst_compression_header_t);
	size = AlignValue<idx_t>(size);
	size += str_len;
	size = AlignValue<idx_t>(size);
	if (fsst_encoded) {
		size += FSST_SYMBOL_TABLE_SIZE2;
		size = AlignValue<idx_t>(size);
	}
	size += string_lengths_space;
	size = AlignValue<idx_t>(size);
	size += dict_indices_space;

	return size;
}

//! A segment cannot be spread out over multiple blocks, so if a string cannot fit in an empty segment
//! the encoding will fail.
bool StringFitsBlock(const idx_t block_size, const idx_t str_len) {
	return RequiredSpace(str_len, false) <= block_size - FSST_SYMBOL_TABLE_SIZE2;
}

//! In the worst case FSST may double the string length by prepending every byte with an exception.
bool FSSTFitsBlock(const idx_t block_size, const idx_t str_len) {
	// +7 as denoted in fsst.h
	return RequiredSpace(str_len * 2 + 7, true) <= block_size;
}

bool DictFSSTAnalyzeState::Analyze(Vector &input, idx_t count) {
	UnifiedVectorFormat vector_format;
	input.ToUnifiedFormat(count, vector_format);
	const auto strings = vector_format.GetData<string_t>(vector_format);
	const auto block_size = info.GetBlockSize();
;
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
			if (!StringFitsBlock(block_size, str_len)) {
				return false;
			}
			if (!FSSTFitsBlock(block_size, str_len)) {
				return false;
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
