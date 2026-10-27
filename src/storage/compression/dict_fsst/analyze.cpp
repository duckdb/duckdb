#include "duckdb/storage/compression/dict_fsst/analyze.hpp"

#include "duckdb/common/bitpacking.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/compression/dict_fsst/common.hpp"

namespace duckdb {
namespace dict_fsst {

//! Determine the size requirements for the worst case, which is when a single string fills an
//! entire segment on its own.
static idx_t GetStringSizeLimit(const idx_t available_space, const bool fsst_encoded) {
	idx_t max_str_len = DictFSSTCompression::STRING_SIZE_LIMIT - 1;
	if (fsst_encoded) {
		// In the worst case FSST may double the string length by prepending every byte with an exception
		max_str_len *= 2;
	}

	// Dictionary contains NULL and current string
	const bitpacking_width_t string_lengths_width = BitpackingPrimitives::MinimumBitWidth(max_str_len);
	const idx_t string_lengths_space = BitpackingPrimitives::GetRequiredSize(2, string_lengths_width);

	// Dictionary stores only one valid string
	const bitpacking_width_t dict_indices_width = BitpackingPrimitives::MinimumBitWidth(1);
	const idx_t dict_indices_space = BitpackingPrimitives::GetRequiredSize(1, dict_indices_width);

	idx_t metadata_size = 0;
	metadata_size += AlignValue<idx_t>(sizeof(dict_fsst_compression_header_t));
	if (fsst_encoded) {
		// As denoted in fsst.h
		metadata_size += 7;
	}
	// Reserve maximum alignment padding for variable length string
	metadata_size += sizeof(idx_t) - 1;
	if (fsst_encoded) {
		metadata_size += AlignValue<idx_t>(DictFSSTCompression::FSST_SYMBOL_TABLE_SIZE);
	}
	metadata_size += AlignValue<idx_t>(string_lengths_space);
	metadata_size += dict_indices_space;

	D_ASSERT(metadata_size < available_space);
	idx_t max_string_size = available_space - metadata_size;

	if (fsst_encoded) {
		max_string_size = max_string_size / 2;
	}

	return MinValue(DictFSSTCompression::STRING_SIZE_LIMIT, max_string_size + 1);
}

DictFSSTAnalyzeState::DictFSSTAnalyzeState(BlockManager &block_manager) : AnalyzeState(block_manager) {
	const auto block_size = info.GetBlockSize();

	string_size_limit = GetStringSizeLimit(block_size, false);
	fsst_string_size_limit = GetStringSizeLimit(block_size, true);
}

bool DictFSSTAnalyzeState::Analyze(const Vector &input) {
	for (auto entry : input.Values<string_t>()) {
		if (!entry.IsValid()) {
			contains_nulls = true;
			continue;
		}
		auto &str = entry.GetValue();
		auto str_len = str.GetSize();
		total_string_length += str_len;
		non_null_count++;
		auto hash = Hash(str);
		distinct_strings.InsertElement(hash);
		if (exact_distinct.size() < EXACT_DISTINCT_LIMIT) {
			if (exact_distinct.insert(hash).second) {
				exact_distinct_length += str_len;
			}
		} else if (exact_distinct.find(hash) == exact_distinct.end()) {
			scaled_string_length += str_len;
			scaled_count++;
		}
		if (str_len > max_string_length) {
			max_string_length = str_len;
		}
		if (str_len >= string_size_limit) {
			// A segment cannot be spread out over multiple blocks, so if a string cannot fit in an empty segment
			// the encoding will fail
			return false;
		}
		if (str_len >= fsst_string_size_limit) {
			// FSST strings may be up to two times larger than their plain equivalent
			disable_fsst = true;
		}
	}
	total_count += input.size();
	return true;
}

idx_t DictFSSTAnalyzeState::FinalAnalyze() {
	// FSST is assumed to halve the strings it encodes
	const double fsst_factor = disable_fsst ? 1.0 : 0.5;
	const auto length_width = BitpackingPrimitives::MinimumBitWidth<idx_t>(max_string_length);

	// Dictionary: every distinct string once (FSST-encoded unless disabled), a bitpacked length per dictionary entry
	// (plus the NULL entry) and a bitpacked dictionary index per row (indices 0..distinct_count, 0 is NULL)
	idx_t distinct_count = 0;
	double distinct_string_length = 0;
	if (non_null_count > 0) {
		distinct_count =
		    MinValue<idx_t>(MaxValue<idx_t>(distinct_strings.Count(), exact_distinct.size()), non_null_count);
		// strings in the exact set count at their real length; the distinct strings beyond it get the average length
		// of the unknown strings that arrived after the set filled up
		distinct_string_length = double(exact_distinct_length);
		if (scaled_count > 0 && distinct_count > exact_distinct.size()) {
			auto extra_distinct = MinValue<idx_t>(distinct_count - exact_distinct.size(), scaled_count);
			distinct_string_length += double(scaled_string_length) * double(extra_distinct) / double(scaled_count);
		}
	}
	const auto index_width = BitpackingPrimitives::MinimumBitWidth<idx_t>(distinct_count);
	double size = distinct_string_length * fsst_factor +
	              double(BitpackingPrimitives::GetRequiredSize(distinct_count + 1, length_width)) +
	              double(BitpackingPrimitives::GetRequiredSize(total_count, index_width));

	// FSST only: when there are no duplicates and no NULLs the strings are stored in row order without indices
	if (!disable_fsst && !contains_nulls) {
		double fsst_only_size = double(total_string_length) * fsst_factor +
		                        double(BitpackingPrimitives::GetRequiredSize(non_null_count, length_width));
		size = MinValue(size, fsst_only_size);
	}
	return LossyNumericCast<idx_t>(size);
}

} // namespace dict_fsst
} // namespace duckdb
