#include "duckdb/storage/compression/dict_fsst/analyze.hpp"

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

idx_t DictFSSTAnalyzeState::RequiredSpace(idx_t tuple_count, idx_t unique_count, idx_t dict_size,
                                          idx_t max_string_length) {
	// index 0 of the dictionary is reserved for NULL
	const idx_t dict_count = unique_count + 1;

	const auto string_lengths_width = BitpackingPrimitives::MinimumBitWidth(max_string_length);
	const auto string_lengths_space = BitpackingPrimitives::GetRequiredSize(dict_count, string_lengths_width);
	const auto dictionary_indices_width = BitpackingPrimitives::MinimumBitWidth(dict_count - 1);
	const auto dictionary_indices_space = BitpackingPrimitives::GetRequiredSize(tuple_count, dictionary_indices_width);

	idx_t required_space = AlignValue<idx_t>(sizeof(dict_fsst_compression_header_t));
	required_space += dict_size;
	required_space = AlignValue<idx_t>(required_space);
	required_space += string_lengths_space;
	required_space = AlignValue<idx_t>(required_space);
	required_space += dictionary_indices_space;
	return required_space;
}

bool DictFSSTAnalyzeState::FitsInBlock(idx_t tuple_count, idx_t unique_count, idx_t dict_size,
                                       idx_t max_str_length) const {
	return RequiredSpace(tuple_count, unique_count, dict_size, max_str_length) <= info.GetBlockSize();
}

void DictFSSTAnalyzeState::FlushSimulatedBlock() {
	segment_count++;
	current_tuple_count = 0;
	current_unique_count = 0;
	current_dict_size = 0;
	current_max_string_length = 0;
	current_set.clear();
	// the strings are only referenced by 'current_set', which we just cleared
	heap.Destroy();
}

bool DictFSSTAnalyzeState::Analyze(const Vector &input) {
	for (auto entry : input.Values<string_t>()) {
		if (!entry.IsValid()) {
			contains_nulls = true;
			// NULL is dictionary index 0, it does not occupy a dictionary entry of its own
			if (!FitsInBlock(current_tuple_count + 1, current_unique_count, current_dict_size,
			                 current_max_string_length)) {
				FlushSimulatedBlock();
			}
			current_tuple_count++;
			continue;
		}
		auto &str = entry.GetValue();
		auto str_len = str.GetSize();
		total_string_length += str_len;
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

		bool new_string = !current_set.count(str);
		if (!new_string) {
			block_had_repeated_value = true;
		}
		auto next_unique_count = current_unique_count + (new_string ? 1 : 0);
		auto next_dict_size = current_dict_size + (new_string ? str_len : 0);
		auto next_max_length = new_string ? MaxValue(current_max_string_length, str_len) : current_max_string_length;

		if (!FitsInBlock(current_tuple_count + 1, next_unique_count, next_dict_size, next_max_length)) {
			FlushSimulatedBlock();
			// the next block starts with an empty dictionary, so this value has to be stored again
			new_string = true;
			next_unique_count = 1;
			next_dict_size = str_len;
			next_max_length = str_len;
		}

		current_tuple_count++;
		current_unique_count = next_unique_count;
		current_dict_size = next_dict_size;
		current_max_string_length = next_max_length;
		if (new_string) {
			current_set.insert(heap.AddBlob(str));
		}
	}
	total_count += input.size();
	return true;
}

idx_t DictFSSTAnalyzeState::FSSTOnlyEstimate() const {
	// FSST_ONLY needs every value in the segment to be unique and no NULLs, see
	// DictFSSTCompressionState::TryEncode. Estimating a mode that cannot be reached would let it undercut the
	// layouts that can.
	if (disable_fsst || contains_nulls || block_had_repeated_value || !total_count) {
		return DConstants::INVALID_INDEX;
	}
	const idx_t block_size = info.GetBlockSize();
	// every block stores the symbol table needed to decode it, and no selection buffer
	const idx_t block_overhead =
	    AlignValue<idx_t>(sizeof(dict_fsst_compression_header_t)) + DictFSSTCompression::FSST_SYMBOL_TABLE_SIZE;
	if (block_overhead >= block_size) {
		return DConstants::INVALID_INDEX;
	}

	// Assume FSST halves the values, which is what it has to achieve to earn back the symbol table. Every value
	// also costs its bitpacked length, counted in bits because the lengths buffer is not byte aligned per value.
	const idx_t encoded_length = MaxValue<idx_t>((total_string_length / 2) / total_count, 1);
	const idx_t bits_per_value = encoded_length * 8 + BitpackingPrimitives::MinimumBitWidth(max_string_length);
	const idx_t values_per_block = ((block_size - block_overhead) * 8) / bits_per_value;
	if (!values_per_block) {
		return DConstants::INVALID_INDEX;
	}
	// charge whole blocks: a block that few large values leave mostly empty still costs a full block
	return ((total_count + values_per_block - 1) / values_per_block) * block_size;
}

idx_t DictFSSTAnalyzeState::FinalAnalyze() {
	if (!total_count) {
		return 0;
	}

	// Every block carries its own dictionary, so a value repeated across blocks is stored once per block.
	// Charging for the blocks the simulation needed also charges for the space wasted in blocks that a large
	// dictionary left mostly empty - which a ratio of the raw size cannot express.
	const idx_t last_block_space =
	    RequiredSpace(current_tuple_count, current_unique_count, current_dict_size, current_max_string_length);
	idx_t estimate = segment_count * info.GetBlockSize() + last_block_space;

	// FSST_ONLY does not deduplicate at all, so it can be the cheaper layout for unique values. It is charged
	// per block as well, otherwise large unique values would again be estimated without paying for the blocks
	// they leave mostly empty.
	const idx_t fsst_only_estimate = FSSTOnlyEstimate();
	if (fsst_only_estimate != DConstants::INVALID_INDEX) {
		estimate = MinValue(estimate, fsst_only_estimate);
	}
	return estimate;
}

} // namespace dict_fsst
} // namespace duckdb
