#include "duckdb/storage/compression/dictionary/decompression.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

void CompressedStringScanState::ValidateDictionary(const SelectionVector &sel, const idx_t scan_count) const {
	D_ASSERT(sel.IsSet());
	bool has_error = false;
	for (idx_t i = 0; i < scan_count; i++) {
		const idx_t sel_idx = sel.get_index_unsafe(i);
		has_error |= sel_idx >= index_buffer_count;
	}

	if (has_error) {
		throw DataCorruptionException(
		    "Failed to scan dictionary string - dictionary index was out of range. Database file appears "
		    "to be corrupted.");
	}
}

void CompressedStringScanState::ValidateIndexBuffer() const {
	// Only the checks required to avoid out-of-bounds reads when trusting the buffer: offsets must be
	// monotonically increasing (else a length underflows) and the largest offset must lie within the dictionary.
	bool has_error = false;
	for (uint32_t i = 1; i < index_buffer_count; i++) {
		has_error |= index_buffer_ptr[i] < index_buffer_ptr[i - 1];
	}
	has_error |= index_buffer_ptr[index_buffer_count - 1] > dict.size;

	if (has_error) {
		throw DataCorruptionException(
		    "Failed to scan dictionary string - dictionary offset was out of range. Database file appears "
		    "to be corrupted.");
	}
}

uint16_t CompressedStringScanState::GetStringLength(sel_t index) {
	if (index == 0) {
		return 0;
	}
	// Offsets are validated up front by ValidateIndexBuffer, so the length can be read directly.
	const auto string_length = index_buffer_ptr[index] - index_buffer_ptr[index - 1];
	return UnsafeNumericCast<uint16_t>(string_length);
}

string_t CompressedStringScanState::FetchStringFromDict(uint32_t dict_offset, uint16_t string_len) {
	if (dict_offset == 0) {
		return string_t(nullptr, 0);
	}

	// normal string: read string from this block
	auto dict_end = baseptr + dict.end;
	auto dict_pos = dict_end - dict_offset;

	auto str_ptr = char_ptr_cast(dict_pos);
	return string_t(str_ptr, string_len);
}

void CompressedStringScanState::Initialize(ColumnSegment &segment, bool initialize_dictionary) {
	block_size = segment.GetBlockSize();
	auto block_offset = segment.GetBlockOffset();
	if (block_offset > block_size || DictionaryCompression::DICTIONARY_HEADER_SIZE > block_size - block_offset) {
		throw DataCorruptionException(
		    "Failed to scan dictionary string - dictionary was out of range. Database file appears to be corrupted.");
	}
	auto segment_capacity = block_size - block_offset;
	baseptr = handle->GetDataMutable() + block_offset;

	// Load header values
	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(baseptr);
	auto index_buffer_offset = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_offset));
	index_buffer_count = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_count));
	auto stored_width = Load<uint32_t>(data_ptr_cast(&header_ptr->bitpacking_width));
	if (index_buffer_count == 0) {
		throw DataCorruptionException(
		    "Failed to scan dictionary string - dictionary was out of range. Database file appears to be corrupted.");
	}
	auto expected_width = BitpackingPrimitives::MinimumBitWidth(index_buffer_count - 1);
	if (stored_width != expected_width) {
		throw DataCorruptionException(
		    "Failed to scan dictionary string - bitpacking width was invalid. Database file appears to be "
		    "corrupted.");
	}
	current_width = expected_width;
	auto selection_buffer_size = BitpackingPrimitives::GetRequiredSize(segment.count.load(), current_width);
	auto expected_index_buffer_offset = DictionaryCompression::DICTIONARY_HEADER_SIZE + selection_buffer_size;
	if (index_buffer_offset != expected_index_buffer_offset) {
		throw DataCorruptionException(
		    "Failed to scan dictionary string - selection buffer was out of range. Database file appears "
		    "to be corrupted.");
	}
	if (index_buffer_offset > segment_capacity ||
	    index_buffer_count > (segment_capacity - index_buffer_offset) / sizeof(uint32_t)) {
		throw DataCorruptionException(
		    "Failed to scan dictionary string - index was out of range. Database file appears to be corrupted.");
	}
	index_buffer_ptr = reinterpret_cast<uint32_t *>(baseptr + index_buffer_offset);
	base_data = data_ptr_cast(baseptr + DictionaryCompression::DICTIONARY_HEADER_SIZE);

	dict = DictionaryCompression::GetDictionary(segment, *handle);
	auto index_buffer_end = index_buffer_offset + sizeof(uint32_t) * index_buffer_count;
	if (dict.end > segment_capacity || dict.size > dict.end || dict.end - dict.size < index_buffer_end) {
		throw DataCorruptionException(
		    "Failed to scan dictionary string - dictionary was out of range. Database file appears to be corrupted.");
	}
	if (!initialize_dictionary) {
		// Used by fetch, as fetch will never produce a DictionaryVector
		return;
	}

	// Validate the whole index buffer once so the dictionary build below can trust it.
	ValidateIndexBuffer();

	dictionary = DictionaryVector::CreateReusableDictionary(segment.GetType(), index_buffer_count);
	dictionary_size = index_buffer_count;
	auto dict_child_data = FlatVector::Writer<string_t>(dictionary->data, index_buffer_count);
	dict_child_data.WriteNull();
	for (uint32_t i = 1; i < index_buffer_count; i++) {
		// NOTE: the passing of dict_child_vector, will not be used, its for big strings
		uint16_t str_len = GetStringLength(i);
		dict_child_data.WriteStringRef(FetchStringFromDict(index_buffer_ptr[i], str_len));
	}
}

template <bool NEEDS_STRING_OFFSET_CHECK>
void CompressedStringScanState::ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count) {
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

	auto result_data = FlatVector::Writer<string_t>(result, scan_count, result_offset);

	bool has_error = false;
	for (idx_t i = 0; i < scan_count; i++) {
		// Lookup dict offset in index buffer
		auto string_dict_index = sel_vec->get_index(i + start_offset);

		bool elem_error = string_dict_index >= index_buffer_count;
		string_dict_index = elem_error ? 0 : string_dict_index;
		auto str_dict_offset = index_buffer_ptr[string_dict_index];

		if (NEEDS_STRING_OFFSET_CHECK) {
			elem_error |= str_dict_offset > dict.size;
			if (string_dict_index > 0) {
				elem_error |= str_dict_offset < index_buffer_ptr[string_dict_index - 1];
			}
			// On error, fall back to index/offset 0 so the fetch below stays in bounds.
			string_dict_index = elem_error ? 0 : string_dict_index;
			str_dict_offset = elem_error ? 0 : str_dict_offset;
		}
		has_error |= elem_error;

		const auto str_len = GetStringLength(UnsafeNumericCast<sel_t>(string_dict_index));
		result_data.WriteStringRef(FetchStringFromDict(str_dict_offset, str_len));
	}

	if (has_error) {
		throw DataCorruptionException(
		    "Failed to scan dictionary string - dictionary index was out of range. Database file appears "
		    "to be corrupted.");
	}
}

template void CompressedStringScanState::ScanToFlatVector<false>(Vector &result, idx_t result_offset, idx_t start,
                                                                 idx_t scan_count);
template void CompressedStringScanState::ScanToFlatVector<true>(Vector &result, idx_t result_offset, idx_t start,
                                                                idx_t scan_count);

void CompressedStringScanState::ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset,
                                                       idx_t start, idx_t scan_count) {
	D_ASSERT(scan_count == STANDARD_VECTOR_SIZE);
	D_ASSERT(result_offset == 0);

	idx_t start_offset = start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
	idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count + start_offset);

	// Create a selection vector of sufficient size if we don't already have one.
	if (!sel_vec || sel_vec_size < decompress_count) {
		sel_vec_size = decompress_count;
		sel_vec = make_buffer<SelectionVector>(decompress_count);
	}

	// Scanning 2048 values, emitting a dict vector
	data_ptr_t dst = data_ptr_cast(sel_vec->data());
	data_ptr_t src = data_ptr_cast(&base_data[((start - start_offset) * current_width) / 8]);

	BitpackingPrimitives::UnPackBuffer<sel_t>(dst, src, decompress_count, current_width);

	sel_vec->ShiftLeft(start_offset, scan_count);
	ValidateDictionary(*sel_vec, scan_count);

	result.Dictionary(dictionary, *sel_vec, scan_count);
}

} // namespace duckdb
