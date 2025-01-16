#include "duckdb/storage/compression/dict_fsst/decompression.hpp"
#include "fsst.h"
#include "duckdb/common/fsst.hpp"

namespace duckdb {
namespace dict_fsst {

CompressedStringScanState::~CompressedStringScanState() {
	delete reinterpret_cast<duckdb_fsst_decoder_t *>(decoder);
}

uint32_t CompressedStringScanState::GetStringLength(sel_t index) {
	return UnsafeNumericCast<uint32_t>(string_lengths[index]);
}

string_t CompressedStringScanState::FetchStringFromDict(Vector &result, int32_t dict_offset, uint32_t string_len) {
	D_ASSERT(dict_offset >= 0 && dict_offset <= NumericCast<int32_t>(block_size));
	if (dict_offset == 0) {
		return string_t(nullptr, 0);
	}

	// normal string: read string from this block
	auto dict_end = baseptr + dict.end;
	auto dict_pos = dict_end - dict_offset;

	auto str_ptr = char_ptr_cast(dict_pos);
	switch (mode) {
	case DictFSSTMode::FSST_ONLY:
	case DictFSSTMode::DICT_FSST:
		return FSSTPrimitives::DecompressValue(decoder, result, str_ptr, string_len, decompress_buffer);
	default:
		// FIXME: the Vector doesn't seem to take ownership of the non-inlined string data???
		return string_t(str_ptr, string_len);
	}
}

void CompressedStringScanState::Initialize(ColumnSegment &segment, bool initialize_dictionary) {
	baseptr = handle->Ptr() + segment.GetBlockOffset();

	// Load header values
	auto header_ptr = reinterpret_cast<dict_fsst_compression_header_t *>(baseptr);
	auto string_lengths_offset = Load<uint32_t>(data_ptr_cast(&header_ptr->string_lengths_offset));
	dict_count = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_count));
	current_width = (bitpacking_width_t)(Load<uint32_t>(data_ptr_cast(&header_ptr->dictionary_indices_width)));
	string_lengths_width = (bitpacking_width_t)(Load<uint32_t>(data_ptr_cast(&header_ptr->string_lengths_width)));
	string_lengths.resize(AlignValue<uint32_t, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE>(dict_count));
	auto string_lengths_size = BitpackingPrimitives::GetRequiredSize(dict_count, string_lengths_width);

	if (segment.GetBlockOffset() + string_lengths_offset + string_lengths_size + header_ptr->dict_size >
	    segment.GetBlockManager().GetBlockSize()) {
		throw IOException(
		    "Failed to scan dictionary string - index was out of range. Database file appears to be corrupted.");
	}
	string_lengths_ptr = baseptr + string_lengths_offset;
	base_data = data_ptr_cast(baseptr + sizeof(dict_fsst_compression_header_t));

	block_size = segment.GetBlockManager().GetBlockSize();

	dict = DictFSSTCompression::GetDictionary(segment, *handle);
	mode = header_ptr->mode;
	if (mode >= DictFSSTMode::COUNT) {
		throw FatalException("This block was written with a mode that is not recognized by this version, highest "
		                     "available mode %d, found mode: %d",
		                     static_cast<uint8_t>(DictFSSTMode::COUNT), static_cast<uint8_t>(mode));
	}

	switch (mode) {
	case DictFSSTMode::FSST_ONLY:
	case DictFSSTMode::DICT_FSST: {
		decoder = new duckdb_fsst_decoder_t;
		auto symbol_table_location = baseptr + dict.end;
		auto ret = duckdb_fsst_import(reinterpret_cast<duckdb_fsst_decoder_t *>(decoder), symbol_table_location);
		(void)(ret);
		D_ASSERT(ret != 0); // FIXME: the old code set the decoder to nullptr instead, why???

		auto string_block_limit = StringUncompressed::GetStringBlockLimit(segment.GetBlockManager().GetBlockSize());
		decompress_buffer.resize(string_block_limit + 1);
		break;
	}
	default:
		break;
	}
	BitpackingPrimitives::UnPackBuffer<uint32_t>(data_ptr_cast(string_lengths.data()),
	                                             data_ptr_cast(string_lengths_ptr), dict_count, string_lengths_width);

	if (!initialize_dictionary) {
		// Used by fetch, as fetch will never produce a DictionaryVector
		return;
	}

	dictionary = make_buffer<Vector>(segment.type, dict_count);
	dictionary_size = dict_count;
	auto dict_child_data = FlatVector::GetData<string_t>(*(dictionary));
	auto &validity = FlatVector::Validity(*dictionary);
	D_ASSERT(dict_count >= 1);
	validity.SetInvalid(0);

	int32_t offset = 0;
	for (uint32_t i = 0; i < dict_count; i++) {
		auto str_len = GetStringLength(i);
		offset += str_len;
		dict_child_data[i] = FetchStringFromDict(*dictionary, offset, str_len);
	}
}

const SelectionVector &CompressedStringScanState::GetSelVec(idx_t start, idx_t scan_count) {
	switch (mode) {
	case DictFSSTMode::FSST_ONLY: {
		return *FlatVector::IncrementalSelectionVector();
	}
	default: {
		// Handling non-bitpacking-group-aligned start values;
		idx_t start_offset = start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		// We will scan in blocks of BITPACKING_ALGORITHM_GROUP_SIZE, so we may scan some extra values.
		idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count + start_offset);

		if (!sel_vec || sel_vec_size < decompress_count) {
			sel_vec_size = decompress_count;
			sel_vec = make_buffer<SelectionVector>(decompress_count);
		}

		data_ptr_t sel_buf_src = &base_data[((start - start_offset) * current_width) / 8];
		sel_t *sel_vec_ptr = sel_vec->data();
		BitpackingPrimitives::UnPackBuffer<sel_t>(data_ptr_cast(sel_vec_ptr), sel_buf_src, decompress_count,
		                                          current_width);
		return *sel_vec;
	}
	}
}

void CompressedStringScanState::ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count) {
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &validity = FlatVector::Validity(result);

	// Create a decompression buffer of sufficient size if we don't already have one.
	auto &selvec = GetSelVec(start, scan_count);

	idx_t start_offset = start;
	if (mode != DictFSSTMode::FSST_ONLY) {
		// Handling non-bitpacking-group-aligned start values;
		start_offset %= BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
	} else {
		//! (index 0 is reserved for NULL, which we don't have in this mode)
		start_offset++;
	}

	if (dictionary) {
		// We have prepared the full dictionary, we can reference these strings directly
		auto dictionary_values = FlatVector::GetData<string_t>(*dictionary);
		for (idx_t i = 0; i < scan_count; i++) {
			// Lookup dict offset in index buffer
			auto string_number = selvec.get_index(i + start_offset);
			if (string_number == 0) {
				validity.SetInvalid(result_offset + i);
			}
			result_data[result_offset + i] = dictionary_values[string_number];
		}
	} else {
		// This path is taken for fetch, where we don't want to decompress the full dictionary
		D_ASSERT(scan_count == 1);

		// Lookup dict offset in index buffer
		auto string_number = selvec.get_index(start_offset);
		if (string_number == 0) {
			validity.SetInvalid(result_offset);
		}

		int32_t offset = 0;
		for (idx_t i = 0; i < string_number; i++) {
			offset += string_lengths[i];
		}
		offset += string_lengths[string_number];

		auto str_len = string_lengths[string_number];
		result_data[result_offset] = FetchStringFromDict(result, offset, str_len);
	}
	result.Verify(result_offset + scan_count);
}

void CompressedStringScanState::ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset,
                                                       idx_t start, idx_t scan_count) {
	D_ASSERT(start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0);
	D_ASSERT(scan_count == STANDARD_VECTOR_SIZE);
	D_ASSERT(result_offset == 0);

	auto &selvec = GetSelVec(start, scan_count);
	result.Dictionary(*(dictionary), dictionary_size, selvec, scan_count);
	DictionaryVector::SetDictionaryId(result, to_string(CastPointerToValue(&segment)));
	result.Verify(result_offset + scan_count);
}

} // namespace dict_fsst
} // namespace duckdb
