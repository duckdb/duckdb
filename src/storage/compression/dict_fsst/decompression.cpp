#include "duckdb/storage/compression/dict_fsst/decompression.hpp"
#include "fsst.h"
#include "duckdb/common/fsst.hpp"

namespace duckdb {
namespace dict_fsst {

CompressedStringScanState::~CompressedStringScanState() {
	delete reinterpret_cast<duckdb_fsst_decoder_t *>(decoder);
}

string_t CompressedStringScanState::FetchStringFromDict(Vector &result, uint32_t dict_offset, idx_t dict_idx) {
	D_ASSERT(dict_offset <= NumericCast<uint32_t>(segment.GetBlockManager().GetBlockSize()));

	if (dict_idx == 0) {
		return string_t(nullptr, 0);
	}
	uint32_t string_len = string_lengths[dict_idx];

	// normal string: read string from this block
	auto dict_pos = dict_ptr + dict_offset;

	auto str_ptr = char_ptr_cast(dict_pos);
	switch (mode) {
	case DictFSSTMode::FSST_ONLY:
	case DictFSSTMode::DICT_FSST: {
		if (string_len == 0) {
			return string_t(nullptr, 0);
		}
		if (all_values_inlined) {
			return FSSTPrimitives::DecompressInlinedValue(decoder, str_ptr, string_len);
		} else {
			return FSSTPrimitives::DecompressValue(decoder, StringVector::GetStringBuffer(result), str_ptr, string_len);
		}
	}
	default:
		// FIXME: the Vector doesn't seem to take ownership of the non-inlined string data???
		return string_t(str_ptr, string_len);
	}
}

void CompressedStringScanState::Initialize(bool initialize_dictionary) {
	baseptr = handle->Ptr() + segment.GetBlockOffset();

	// Load header values
	auto header_ptr = reinterpret_cast<dict_fsst_compression_header_t *>(baseptr);
	mode = header_ptr->mode;
	if (mode >= DictFSSTMode::COUNT) {
		throw FatalException("This block was written with a mode that is not recognized by this version, highest "
		                     "available mode %d, found mode: %d",
		                     static_cast<uint8_t>(DictFSSTMode::COUNT), static_cast<uint8_t>(mode));
	}

	dict_count = header_ptr->dict_count;
	auto symbol_table_size = header_ptr->symbol_table_size;
	dictionary_size = header_ptr->dict_size;

	dictionary_indices_width =
	    (bitpacking_width_t)(Load<uint8_t>(data_ptr_cast(&header_ptr->dictionary_indices_width)));
	string_lengths_width = (bitpacking_width_t)(Load<uint8_t>(data_ptr_cast(&header_ptr->string_lengths_width)));

	auto string_lengths_space = BitpackingPrimitives::GetRequiredSize(dict_count, string_lengths_width);
	auto dictionary_indices_space =
	    BitpackingPrimitives::GetRequiredSize(segment.count.load(), dictionary_indices_width);

	auto dictionary_dest = AlignValue<idx_t>(DictFSSTCompression::DICTIONARY_HEADER_SIZE);
	auto symbol_table_dest = AlignValue<idx_t>(dictionary_dest + dictionary_size);
	auto string_lengths_dest = AlignValue<idx_t>(symbol_table_dest + symbol_table_size);
	auto dictionary_indices_dest = AlignValue<idx_t>(string_lengths_dest + string_lengths_space);

	const auto total_space = segment.GetBlockOffset() + dictionary_indices_dest + dictionary_indices_space;
	if (total_space > segment.GetBlockManager().GetBlockSize()) {
		throw IOException(
		    "Failed to scan dictionary string - index was out of range. Database file appears to be corrupted.");
	}
	dict_ptr = data_ptr_cast(baseptr + dictionary_dest);
	dictionary_indices_ptr = data_ptr_cast(baseptr + dictionary_indices_dest);
	string_lengths_ptr = data_ptr_cast(baseptr + string_lengths_dest);

	switch (mode) {
	case DictFSSTMode::FSST_ONLY:
	case DictFSSTMode::DICT_FSST: {
		decoder = new duckdb_fsst_decoder_t;
		auto ret = duckdb_fsst_import(reinterpret_cast<duckdb_fsst_decoder_t *>(decoder), baseptr + symbol_table_dest);
		(void)(ret);
		D_ASSERT(ret != 0); // FIXME: the old code set the decoder to nullptr instead, why???
		break;
	}
	default:
		break;
	}

	string_lengths.resize(AlignValue<uint32_t, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE>(dict_count));
	BitpackingPrimitives::UnPackBuffer<uint32_t>(data_ptr_cast(string_lengths.data()),
	                                             data_ptr_cast(string_lengths_ptr), dict_count, string_lengths_width);
	if (!initialize_dictionary || mode == DictFSSTMode::FSST_ONLY) {
		// Used by fetch, as fetch will never produce a DictionaryVector
		return;
	}

	dictionary = DictionaryVector::CreateReusableDictionary(segment.type, dict_count);
	auto dict_child_data = FlatVector::GetData<string_t>(dictionary->data);
	auto &validity = FlatVector::Validity(dictionary->data);
	D_ASSERT(dict_count >= 1);
	validity.SetInvalid(0);

	auto &dict_data = dictionary->data;
	uint32_t offset = 0;
	for (uint32_t i = 0; i < dict_count; i++) {
		//! We can uncompress during fetching, we need the length of the string inside the dictionary
		auto string_len = string_lengths[i];
		dict_child_data[i] = FetchStringFromDict(dict_data, offset, i);
		offset += string_len;
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

		data_ptr_t sel_buf_src = &dictionary_indices_ptr[((start - start_offset) * dictionary_indices_width) / 8];
		sel_t *sel_vec_ptr = sel_vec->data();
		BitpackingPrimitives::UnPackBuffer<sel_t>(data_ptr_cast(sel_vec_ptr), sel_buf_src, decompress_count,
		                                          dictionary_indices_width);

		if (start_offset != 0) {
			for (idx_t i = 0; i < scan_count; i++) {
				sel_vec->set_index(i, sel_vec->get_index(i + start_offset));
			}
		}

		return *sel_vec;
	}
	}
}

void CompressedStringScanState::ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count) {
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &validity = FlatVector::Validity(result);

	// Create a decompression buffer of sufficient size if we don't already have one.
	auto &selvec = GetSelVec(start, scan_count);

	//! (index 0 is reserved for NULL, which we don't have in this mode)
	const idx_t start_offset = mode == DictFSSTMode::FSST_ONLY ? start + 1 : 0;

	if (dictionary) {
		// We have prepared the full dictionary, we can reference these strings directly
		auto dictionary_values = FlatVector::GetData<string_t>(dictionary->data);
		for (idx_t i = 0; i < scan_count; i++) {
			// Lookup dict offset in index buffer
			auto string_number = selvec.get_index(i + start_offset);
			if (string_number == 0) {
				validity.SetInvalid(result_offset + i);
			}
			result_data[result_offset + i] = dictionary_values[string_number];
		}
	} else {
		for (idx_t i = 0; i < scan_count; i++) {
			// Lookup dict offset in index buffer
			auto string_number = selvec.get_index(start_offset + i);
			if (string_number == 0) {
				validity.SetInvalid(result_offset);
			}
			if (decompress_position > string_number) {
				throw InternalException("DICT_FSST: not performing a sequential scan?");
			}
			for (; decompress_position < string_number; decompress_position++) {
				decompress_offset += string_lengths[decompress_position];
			}
			result_data[result_offset + i] = FetchStringFromDict(result, decompress_offset, string_number);
		}
	}
	result.Verify(result_offset + scan_count);
}

void CompressedStringScanState::Select(Vector &result, idx_t start, const SelectionVector &sel, idx_t sel_count) {
	D_ASSERT(!dictionary);
	D_ASSERT(mode == DictFSSTMode::FSST_ONLY);
	idx_t start_offset = start + 1;
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < sel_count; i++) {
		// Lookup dict offset in index buffer
		auto string_number = start_offset + sel.get_index(i);
		if (decompress_position > string_number) {
			throw InternalException("DICT_FSST: not performing a sequential scan?");
		}
		for (; decompress_position < string_number; decompress_position++) {
			decompress_offset += string_lengths[decompress_position];
		}
		result_data[i] = FetchStringFromDict(result, decompress_offset, string_number);
	}
}

bool CompressedStringScanState::AllowDictionaryScan(idx_t scan_count) {
	if (mode == DictFSSTMode::FSST_ONLY) {
		return false;
	}
	if (scan_count != STANDARD_VECTOR_SIZE) {
		return false;
	}
	if (!dictionary) {
		return false;
	}
	return true;
}

void CompressedStringScanState::ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset,
                                                       idx_t start, idx_t scan_count) {
	D_ASSERT(scan_count == STANDARD_VECTOR_SIZE);
	D_ASSERT(result_offset == 0);

	auto &selvec = GetSelVec(start, scan_count);
	result.Dictionary(dictionary, selvec);
	result.Verify(result_offset + scan_count);
}

} // namespace dict_fsst
} // namespace duckdb
