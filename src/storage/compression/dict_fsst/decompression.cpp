#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/storage/compression/dict_fsst/decompression.hpp"
#include "fsst.h"
#include "duckdb/common/fsst.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {
namespace dict_fsst {

CompressedStringScanState::~CompressedStringScanState() {
	delete reinterpret_cast<duckdb_fsst_decoder_t *>(decoder);
}

string_t CompressedStringScanState::FetchStringFromDict(Vector &result, uint32_t dict_offset, idx_t dict_idx,
                                                        StringScanType string_scan_type) {
	D_ASSERT(dict_offset <= NumericCast<uint32_t>(segment.GetBlockSize()));

	if (dict_idx == 0) {
		return string_t(nullptr, 0);
	}
	uint32_t string_len = GetStringLength(dict_idx);

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
			return FSSTPrimitives::DecompressValue(decoder, StringVector::GetStringAllocator(result), str_ptr,
			                                       string_len);
		}
	}
	default:
		if (string_scan_type == StringScanType::COPY_STRINGS) {
			return StringVector::AddStringOrBlob(result, string_t(str_ptr, string_len));
		}
		// FIXME: the Vector doesn't seem to take ownership of the non-inlined string data???
		return string_t(str_ptr, string_len);
	}
}

void CompressedStringScanState::Initialize(bool initialize_dictionary) {
	baseptr = handle.GetDataMutable() + segment.GetBlockOffset();

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
	if (dictionary_indices_width > sizeof(uint32_t) * 8 || string_lengths_width > sizeof(uint32_t) * 8) {
		throw IOException("Failed to scan dictionary string - bitpacking width was invalid. Database file appears to "
		                  "be corrupted.");
	}

	auto string_lengths_space = BitpackingPrimitives::GetRequiredSize(dict_count, string_lengths_width);
	auto dictionary_indices_space =
	    BitpackingPrimitives::GetRequiredSize(segment.count.load(), dictionary_indices_width);

	auto dictionary_dest = AlignValue<idx_t>(DictFSSTCompression::DICTIONARY_HEADER_SIZE);
	auto symbol_table_dest = AlignValue<idx_t>(dictionary_dest + dictionary_size);
	auto string_lengths_dest = AlignValue<idx_t>(symbol_table_dest + symbol_table_size);
	auto dictionary_indices_dest = AlignValue<idx_t>(string_lengths_dest + string_lengths_space);

	const auto total_space = segment.GetBlockOffset() + dictionary_indices_dest + dictionary_indices_space;
	if (total_space > segment.GetBlockSize()) {
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
		// in FSST_ONLY / DICT_FSST modes a symbol table is always present, so any failure (out-of-bounds or a
		// version mismatch) means the segment is corrupted
		auto ret = duckdb_fsst_import(reinterpret_cast<duckdb_fsst_decoder_t *>(decoder), baseptr + symbol_table_dest,
		                              symbol_table_size);
		if (ret == DUCKDB_FSST_IMPORT_VERSION_MISMATCH || ret == DUCKDB_FSST_IMPORT_OUT_OF_BOUNDS) {
			throw IOException("Failed to scan DICT_FSST string segment: invalid FSST symbol table. Database file "
			                  "appears to be corrupted.");
		}
		break;
	}
	default:
		break;
	}

	// scans build the dictionary on first use
	build_dictionary = initialize_dictionary && mode != DictFSSTMode::FSST_ONLY;
}

void CompressedStringScanState::LoadStringLengths() {
	if (!string_lengths.empty()) {
		return;
	}
	string_lengths.resize(AlignValue<uint32_t, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE>(dict_count));
	BitpackingPrimitives::UnPackBuffer<uint32_t>(data_ptr_cast(string_lengths.data()),
	                                             data_ptr_cast(string_lengths_ptr), dict_count, string_lengths_width);
}

const uint32_t *CompressedStringScanState::GetStringLengthGroup(idx_t group_start) {
	D_ASSERT(group_start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0 && group_start < dict_count);
	if (string_length_group_start != group_start) {
		BitpackingPrimitives::UnPackBuffer<uint32_t>(
		    data_ptr_cast(string_length_group), string_lengths_ptr + group_start * string_lengths_width / 8,
		    BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE, string_lengths_width);
		string_length_group_start = group_start;
	}
	return string_length_group;
}

uint32_t CompressedStringScanState::GetStringLength(idx_t dict_idx) {
	if (!string_lengths.empty()) {
		return string_lengths[dict_idx];
	}
	const auto group_offset = dict_idx % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
	return GetStringLengthGroup(dict_idx - group_offset)[group_offset];
}

uint32_t CompressedStringScanState::SumStringLengths(idx_t start, idx_t end) {
	uint32_t sum = 0;
	if (!string_lengths.empty()) {
		const auto lengths = string_lengths.data();
		for (auto i = start; i < end; i++) {
			sum += lengths[i];
		}
		return sum;
	}
	constexpr idx_t GROUP_SIZE = BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
	for (auto group_start = start - start % GROUP_SIZE; group_start < end; group_start += GROUP_SIZE) {
		const auto lengths = GetStringLengthGroup(group_start);
		const auto to = MinValue<idx_t>(end - group_start, GROUP_SIZE);
		for (auto i = MaxValue<idx_t>(start, group_start) - group_start; i < to; i++) {
			sum += lengths[i];
		}
	}
	return sum;
}

bool CompressedStringScanState::EnsureDictionary() {
	if (dictionary || !build_dictionary) {
		return dictionary != nullptr;
	}
	dictionary = DictionaryVector::CreateReusableDictionary(segment.GetType(), dict_count);
	auto &dict_data = dictionary->data;
	auto dict_child_data = FlatVector::GetDataMutable<string_t>(dict_data);
	auto &validity = FlatVector::ValidityMutable(dict_data);
	D_ASSERT(dict_count >= 1);
	validity.SetInvalid(0);
	LoadStringLengths();

	uint32_t offset = 0;
	for (uint32_t i = 0; i < dict_count; i++) {
		//! We can uncompress during fetching, we need the length of the string inside the dictionary
		auto string_len = string_lengths[i];
		dict_child_data[i] = FetchStringFromDict(dict_data, offset, i);
		offset += string_len;
	}
	return true;
}

uint32_t CompressedStringScanState::GetStringOffset(idx_t string_number, bool allow_restart) {
	if (string_number >= dict_count) {
		throw DataCorruptionException("Failed to scan dictionary string - dictionary index was out of range. Database "
		                              "file appears to be corrupted.");
	}
	if (!string_offsets.empty()) {
		return string_offsets[string_number];
	}
	if (string_number < decompress_position) {
		if (!allow_restart || summed_lengths >= dict_count) {
			LoadStringLengths();
			string_offsets.resize(dict_count);
			uint32_t offset = 0;
			for (uint32_t i = 0; i < dict_count; i++) {
				string_offsets[i] = offset;
				offset += string_lengths[i];
			}
			return string_offsets[string_number];
		}
		decompress_position = 0;
		decompress_offset = 0;
	}
	decompress_offset += SumStringLengths(decompress_position, string_number);
	summed_lengths += string_number - decompress_position;
	decompress_position = string_number;
	return decompress_offset;
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

		sel_vec->ShiftLeft(start_offset, scan_count);

		return *sel_vec;
	}
	}
}

void CompressedStringScanState::ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count,
                                                 StringScanType string_scan_type) {
	Select(result, result_offset, start, *FlatVector::IncrementalSelectionVector(), scan_count, string_scan_type);
	result.Verify();
}

void CompressedStringScanState::Select(Vector &result, idx_t result_offset, idx_t start, const SelectionVector &sel,
                                       idx_t sel_count, StringScanType string_scan_type) {
	const auto scan_count = sel_count == 0 ? 0 : sel.get_index(sel_count - 1) + 1;
	auto &selvec = GetSelVec(start, scan_count);

	//! (index 0 is reserved for NULL, which we don't have in this mode)
	const idx_t start_offset = mode == DictFSSTMode::FSST_ONLY ? start + 1 : 0;

	auto result_data = FlatVector::Writer<string_t>(result, sel_count, result_offset);
	if (string_scan_type == StringScanType::COPY_STRINGS) {
		// a select of many rows unpacks every length up front and computes all offsets on its first step back
		const bool few_rows = sel_count < BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		if (!few_rows) {
			LoadStringLengths();
		}
		for (idx_t i = 0; i < sel_count; i++) {
			auto string_number = selvec.get_index(start_offset + sel.get_index(i));
			if (string_number == 0) {
				result_data.WriteNull();
				continue;
			}
			result_data.WriteStringRef(FetchStringFromDict(result, GetStringOffset(string_number, few_rows),
			                                               string_number, StringScanType::COPY_STRINGS));
		}
	} else if (EnsureDictionary()) {
		// We have prepared the full dictionary, we can reference these strings directly
		auto dictionary_values = FlatVector::GetData<string_t>(dictionary->data);
		for (idx_t i = 0; i < sel_count; i++) {
			// Lookup dict offset in index buffer
			auto string_number = selvec.get_index(start_offset + sel.get_index(i));
			if (string_number == 0) {
				result_data.WriteNull();
				continue;
			}
			result_data.WriteStringRef(dictionary_values[string_number]);
		}
	} else {
		LoadStringLengths();
		for (idx_t i = 0; i < sel_count; i++) {
			// Lookup dict offset in index buffer
			auto string_number = selvec.get_index(start_offset + sel.get_index(i));
			if (string_number == 0) {
				result_data.WriteNull();
				continue;
			}
			if (decompress_position > string_number) {
				throw InternalException("DICT_FSST: not performing a sequential scan?");
			}
			for (; decompress_position < string_number; decompress_position++) {
				decompress_offset += string_lengths[decompress_position];
			}
			result_data.WriteStringRef(FetchStringFromDict(result, decompress_offset, string_number));
		}
	}
}

bool CompressedStringScanState::AllowDictionaryScan(idx_t scan_count) {
	if (mode == DictFSSTMode::FSST_ONLY) {
		return false;
	}
	if (scan_count != STANDARD_VECTOR_SIZE) {
		return false;
	}
	return EnsureDictionary();
}

void CompressedStringScanState::ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset,
                                                       idx_t start, idx_t scan_count) {
	D_ASSERT(scan_count == STANDARD_VECTOR_SIZE);
	D_ASSERT(result_offset == 0);

	auto &selvec = GetSelVec(start, scan_count);
	result.Dictionary(dictionary, selvec, scan_count);
	result.Verify();
}

} // namespace dict_fsst
} // namespace duckdb
