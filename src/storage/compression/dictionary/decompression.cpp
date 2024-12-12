#include "duckdb/storage/compression/dictionary/decompression.hpp"

namespace duckdb {

void CompressedStringScanState::Initialize(ColumnSegment &segment, bool initialize_dictionary) {
	baseptr = handle->Ptr() + segment.GetBlockOffset();

	// Load header values
	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(baseptr);
	auto index_buffer_offset = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_offset));
	index_buffer_count = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_count));
	current_width = (bitpacking_width_t)(Load<uint32_t>(data_ptr_cast(&header_ptr->bitpacking_width)));
	if (segment.GetBlockOffset() + index_buffer_offset + sizeof(uint32_t) * index_buffer_count >
	    segment.GetBlockManager().GetBlockSize()) {
		throw IOException(
		    "Failed to scan dictionary string - index was out of range. Database file appears to be corrupted.");
	}
	index_buffer_ptr = reinterpret_cast<uint32_t *>(baseptr + index_buffer_offset);
	base_data = data_ptr_cast(baseptr + DictionaryCompression::DICTIONARY_HEADER_SIZE);

	if (!initialize_dictionary) {
		// Used by fetch, as fetch will never produce a DictionaryVector
		return;
	}

	dict = DictionaryCompression::GetDictionary(segment, *handle);
	dictionary = make_buffer<Vector>(segment.type, index_buffer_count);
	dictionary_size = index_buffer_count;
	auto dict_child_data = FlatVector::GetData<string_t>(*(dictionary));

	for (uint32_t i = 0; i < index_buffer_count; i++) {
		// NOTE: the passing of dict_child_vector, will not be used, its for big strings
		uint16_t str_len = DictionaryCompression::GetStringLength(index_buffer_ptr, i);
		dict_child_data[i] = DictionaryCompression::FetchStringFromDict(
		    segment, dict, baseptr, UnsafeNumericCast<int32_t>(index_buffer_ptr[i]), str_len);
	}
}

void CompressedStringScanState::ScanToFlatVector(ColumnSegment &segment, Vector &result, idx_t result_offset,
                                                 idx_t start, idx_t scan_count) {
	auto result_data = FlatVector::GetData<string_t>(result);

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
		auto dict_offset = index_buffer_ptr[string_number];
		auto str_len =
		    DictionaryCompression::GetStringLength(index_buffer_ptr, UnsafeNumericCast<sel_t>(string_number));
		result_data[result_offset + i] = DictionaryCompression::FetchStringFromDict(
		    segment, dict, baseptr, UnsafeNumericCast<int32_t>(dict_offset), str_len);
	}
}

void CompressedStringScanState::ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset,
                                                       idx_t start, idx_t scan_count) {
	auto result_data = FlatVector::GetData<string_t>(result);

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
	DictionaryVector::SetDictionaryId(result, to_string(CastPointerToValue(&segment)));
}

} // namespace duckdb
