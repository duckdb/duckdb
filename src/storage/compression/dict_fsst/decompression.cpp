#include "duckdb/storage/compression/dict_fsst/decompression.hpp"
#include "fsst.h"
#include "duckdb/common/fsst.hpp"

namespace duckdb {
namespace dict_fsst {

CompressedStringScanState::~CompressedStringScanState() {
	delete reinterpret_cast<duckdb_fsst_decoder_t *>(decoder);
}

uint16_t CompressedStringScanState::GetStringLength(sel_t index) {
	if (index == 0) {
		return 0;
	} else {
		return UnsafeNumericCast<uint16_t>(index_buffer_ptr[index] - index_buffer_ptr[index - 1]);
	}
}

string_t CompressedStringScanState::FetchStringFromDict(Vector &result, int32_t dict_offset, uint16_t string_len) {
	D_ASSERT(dict_offset >= 0 && dict_offset <= NumericCast<int32_t>(block_size));
	if (dict_offset == 0) {
		return string_t(nullptr, 0);
	}

	// normal string: read string from this block
	auto dict_end = baseptr + dict.end;
	auto dict_pos = dict_end - dict_offset;

	auto str_ptr = char_ptr_cast(dict_pos);
	if (is_fsst_encoded) {
		return FSSTPrimitives::DecompressValue(decoder, result, str_ptr, string_len, decompress_buffer);
	} else {
		// FIXME: the Vector doesn't seem to take ownership of the non-inlined string data???
		return string_t(str_ptr, string_len);
	}
}

void CompressedStringScanState::Initialize(ColumnSegment &segment, bool initialize_dictionary) {
	baseptr = handle->Ptr() + segment.GetBlockOffset();

	// Load header values
	auto header_ptr = reinterpret_cast<dict_fsst_compression_header_t *>(baseptr);
	auto index_buffer_offset = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_offset));
	index_buffer_count = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_count));
	current_width = (bitpacking_width_t)(Load<uint32_t>(data_ptr_cast(&header_ptr->bitpacking_width)));
	if (segment.GetBlockOffset() + index_buffer_offset + sizeof(uint32_t) * index_buffer_count >
	    segment.GetBlockManager().GetBlockSize()) {
		throw IOException(
		    "Failed to scan dictionary string - index was out of range. Database file appears to be corrupted.");
	}
	index_buffer_ptr = reinterpret_cast<uint32_t *>(baseptr + index_buffer_offset);
	base_data = data_ptr_cast(baseptr + DictFSSTCompression::DICTIONARY_HEADER_SIZE);

	block_size = segment.GetBlockManager().GetBlockSize();

	dict = DictFSSTCompression::GetDictionary(segment, *handle);
	is_fsst_encoded = header_ptr->fsst_encoded;
	if (is_fsst_encoded) {
		decoder = new duckdb_fsst_decoder_t;
		auto symbol_table_location = baseptr + dict.end;
		auto ret = duckdb_fsst_import(reinterpret_cast<duckdb_fsst_decoder_t *>(decoder), symbol_table_location);
		(void)(ret);
		D_ASSERT(ret != 0); // FIXME: the old code set the decoder to nullptr instead, why???

		auto string_block_limit = StringUncompressed::GetStringBlockLimit(segment.GetBlockManager().GetBlockSize());
		decompress_buffer.resize(string_block_limit + 1);
	}

	if (!initialize_dictionary) {
		// Used by fetch, as fetch will never produce a DictionaryVector
		return;
	}

	dictionary = make_buffer<Vector>(segment.type, index_buffer_count);
	dictionary_size = index_buffer_count;
	auto dict_child_data = FlatVector::GetData<string_t>(*(dictionary));
	auto &validity = FlatVector::Validity(*dictionary);
	D_ASSERT(index_buffer_count >= 1);
	validity.SetInvalid(0);

	for (uint32_t i = 0; i < index_buffer_count; i++) {
		auto str_len = GetStringLength(i);
		dict_child_data[i] = FetchStringFromDict(*dictionary, UnsafeNumericCast<int32_t>(index_buffer_ptr[i]), str_len);
	}
}

void CompressedStringScanState::ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count) {
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &validity = FlatVector::Validity(result);

	// Handling non-bitpacking-group-aligned start values;
	idx_t start_offset = start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	// We will scan in blocks of BITPACKING_ALGORITHM_GROUP_SIZE, so we may scan some extra values.
	idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count + start_offset);

	// Create a decompression buffer of sufficient size if we don't already have one.
	if (!sel_vec || sel_vec_size < decompress_count) {
		sel_vec_size = decompress_count;
		sel_vec = make_buffer<SelectionVector>(decompress_count);
	}

	data_ptr_t src = &base_data[((start - start_offset) * current_width) / 8];
	sel_t *sel_vec_ptr = sel_vec->data();

	BitpackingPrimitives::UnPackBuffer<sel_t>(data_ptr_cast(sel_vec_ptr), src, decompress_count, current_width);

	for (idx_t i = 0; i < scan_count; i++) {
		// Lookup dict offset in index buffer
		auto string_number = sel_vec->get_index(i + start_offset);
		if (string_number == 0) {
			validity.SetInvalid(result_offset + i);
		}
		auto dict_offset = index_buffer_ptr[string_number];
		auto str_len = GetStringLength(UnsafeNumericCast<sel_t>(string_number));
		result_data[result_offset + i] = FetchStringFromDict(result, UnsafeNumericCast<int32_t>(dict_offset), str_len);
	}
}

void CompressedStringScanState::ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset,
                                                       idx_t start, idx_t scan_count) {
	D_ASSERT(start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0);
	D_ASSERT(scan_count == STANDARD_VECTOR_SIZE);
	D_ASSERT(result_offset == 0);

	idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count);

	// Create a selection vector of sufficient size if we don't already have one.
	if (!sel_vec || sel_vec_size < decompress_count) {
		sel_vec_size = decompress_count;
		sel_vec = make_buffer<SelectionVector>(decompress_count);
	}

	// Scanning 2048 values, emitting a dict vector
	data_ptr_t dst = data_ptr_cast(sel_vec->data());
	data_ptr_t src = data_ptr_cast(&base_data[(start * current_width) / 8]);

	BitpackingPrimitives::UnPackBuffer<sel_t>(dst, src, scan_count, current_width);

	result.Dictionary(*(dictionary), dictionary_size, *sel_vec, scan_count);
	// FIXME: this assumes the type is VectorType::DICTIONARY
	// this could fail if the result is a ConstantVector instead.
	DictionaryVector::SetDictionaryId(result, to_string(CastPointerToValue(&segment)));
}

} // namespace dict_fsst
} // namespace duckdb
