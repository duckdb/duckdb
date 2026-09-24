#include "duckdb/storage/compression/dictionary/decompression.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Error Helpers
//===--------------------------------------------------------------------===//
[[noreturn]] static void ThrowDictionaryIndexOutOfRange() {
	throw DataCorruptionException(
	    "Failed to scan dictionary string - dictionary index was out of range. Database file appears "
	    "to be corrupted.");
}

[[noreturn]] static void ThrowDictionaryOffsetOutOfRange() {
	throw DataCorruptionException(
	    "Failed to scan dictionary string - dictionary offset was out of range. Database file appears "
	    "to be corrupted.");
}

[[noreturn]] static void ThrowDictionaryOutOfRange() {
	throw DataCorruptionException(
	    "Failed to scan dictionary string - dictionary was out of range. Database file appears to be corrupted.");
}

[[noreturn]] static void ThrowDictionaryBitpackingWidthInvalid() {
	throw DataCorruptionException(
	    "Failed to scan dictionary string - bitpacking width was invalid. Database file appears to be "
	    "corrupted.");
}

[[noreturn]] static void ThrowDictionarySelectionBufferOutOfRange() {
	throw DataCorruptionException(
	    "Failed to scan dictionary string - selection buffer was out of range. Database file appears "
	    "to be corrupted.");
}

//===--------------------------------------------------------------------===//
// Dictionary Validation
//===--------------------------------------------------------------------===//
void CompressedStringScanState::DictionarySegmentLayout::ValidateDictionary(const SelectionVector &sel,
                                                                            const idx_t scan_count) const {
	D_ASSERT(sel.IsSet());
	bool has_error = false;
	for (idx_t i = 0; i < scan_count; i++) {
		const idx_t sel_idx = sel.get_index_unsafe(i);
		has_error |= sel_idx >= index_buffer.size();
	}

	if (has_error) {
		ThrowDictionaryIndexOutOfRange();
	}
}

void CompressedStringScanState::DictionarySegmentLayout::ValidateIndexBuffer() const {
	// Only the checks required to avoid out-of-bounds reads when trusting the buffer: offsets must be
	// monotonically increasing (else a length underflows) and the largest offset must lie within the dictionary.
	const auto &offsets = index_buffer;
	bool has_error = false;
	for (idx_t i = 1; i < offsets.size(); i++) {
		has_error |= offsets[i] < offsets[i - 1];
	}
	has_error |= offsets[offsets.size() - 1] > dictionary_reader.Size();

	if (has_error) {
		ThrowDictionaryOffsetOutOfRange();
	}
}

//===--------------------------------------------------------------------===//
// String Reading
//===--------------------------------------------------------------------===//
uint32_t CompressedStringScanState::DictionarySegmentLayout::GetStringLength(idx_t index) const {
	const auto &offsets = index_buffer;
	D_ASSERT(index < offsets.size());
	if (index == 0) {
		return 0;
	}
	D_ASSERT(offsets[index] >= offsets[index - 1]);
	const auto string_length = offsets[index] - offsets[index - 1];
	return string_length;
}

string_t CompressedStringScanState::DictionarySegmentLayout::FetchStringFromDict(uint32_t dict_offset,
                                                                                 uint32_t string_len) const {
	D_ASSERT(dict_offset <= dictionary_reader.Size());
	D_ASSERT(string_len <= dict_offset);
	if (dict_offset == 0) {
		return string_t(nullptr, 0);
	}

	// normal string: read string from this block
	auto string_data = dictionary_reader.GetBytes(dictionary_reader.Size() - dict_offset, string_len);
	return string_t(const_char_ptr_cast(string_data.data()), string_len);
}

//===--------------------------------------------------------------------===//
// Segment Layout
//===--------------------------------------------------------------------===//
CompressedStringScanState::DictionarySegmentLayout CompressedStringScanState::ReadLayout(const BufferHandle &handle,
                                                                                         const ColumnSegment &segment) {
	auto reader = CompressionSegmentReader::FromSegment(handle, segment, "dictionary segment");
	auto header = reader.Read<dictionary_compression_header_t>();
	// Index zero represents NULL, so even an all-NULL segment needs one dictionary offset.
	if (header.index_buffer_count == 0) {
		ThrowDictionaryOutOfRange();
	}
	// Selections refer to entries in the offset table, so the last index determines the bit width.
	auto expected_width = BitpackingPrimitives::MinimumBitWidth(header.index_buffer_count - 1);
	if (header.bitpacking_width != expected_width) {
		ThrowDictionaryBitpackingWidthInvalid();
	}
	// The offset table must start at (or after) the header's end.
	if (header.index_buffer_offset < reader.Position()) {
		ThrowDictionarySelectionBufferOutOfRange();
	}
	auto selection_reader =
	    reader.ReadSubReader(header.index_buffer_offset - reader.Position(), "dictionary selections");

	constexpr auto group_size = BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
	const auto row_count = segment.count.load();
	// Count the final partial group without rounding up the row count, which could overflow.
	const auto group_count = row_count / group_size + (row_count % group_size != 0);
	const auto group_bytes = group_size * expected_width / 8;
	if (group_bytes == 0) {
		// With only the NULL entry to select, no bits are encoded.
		// The selection reader must therefore be empty, otherwise its size conflicts with the zero bit width.
		if (selection_reader.Size() != 0) {
			ThrowDictionarySelectionBufferOutOfRange();
		}
	} else {
		// Require exactly enough whole groups for the rows. Compare by division to avoid overflowing
		if (selection_reader.Size() % group_bytes != 0 || selection_reader.Size() / group_bytes != group_count) {
			ThrowDictionarySelectionBufferOutOfRange();
		}
	}
	// Dictionary bytes grow backwards from dict_end, so dict_size must not exceed it.
	if (header.dict_size > header.dict_end) {
		ThrowDictionaryOutOfRange();
	}

	// Check that the offset table is aligned, fits within the segment and does not overlap the dictionary.
	// Defer checking its values so fetching one string does not require validating every offset.
	auto index_buffer = reader.GetArray<uint32_t>(header.index_buffer_offset, header.index_buffer_count);
	auto index_buffer_end = header.index_buffer_offset + sizeof(uint32_t) * index_buffer.size();
	if (header.dict_end - header.dict_size < index_buffer_end) {
		ThrowDictionaryOutOfRange();
	}
	// Check that the dictionary fits within the reader, then restrict string reads to its bytes.
	auto dictionary_reader =
	    reader.GetSubReader(header.dict_end - header.dict_size, header.dict_size, "dictionary strings");
	return {expected_width, selection_reader, dictionary_reader, index_buffer};
}

string_t CompressedStringScanState::DictionarySegmentLayout::ValidateAndGetEntry(idx_t index) const {
	if (index >= index_buffer.size()) {
		ThrowDictionaryIndexOutOfRange();
	}
	auto offset = index_buffer[index];
	if (offset > dictionary_reader.Size()) {
		ThrowDictionaryOffsetOutOfRange();
	}
	if (index == 0) {
		return string_t(nullptr, 0);
	}
	auto previous_offset = index_buffer[index - 1];
	if (offset < previous_offset) {
		ThrowDictionaryOffsetOutOfRange();
	}
	const auto string_length = offset - previous_offset;
	auto string_data = dictionary_reader.GetBytes(dictionary_reader.Size() - offset, string_length);
	return string_t(const_char_ptr_cast(string_data.data()), string_length);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void CompressedStringScanState::InitializeDictionary(const ColumnSegment &segment) {
	// Validate the whole index buffer once so the dictionary build below can trust it.
	layout.ValidateIndexBuffer();

	const auto &offsets = layout.index_buffer;
	dictionary = DictionaryVector::CreateReusableDictionary(segment.GetType(), offsets.size());
	auto dict_child_data = FlatVector::Writer<string_t>(dictionary->data, offsets.size());
	dict_child_data.WriteNull();
	for (idx_t i = 1; i < offsets.size(); i++) {
		const auto str_len = layout.GetStringLength(i);
		dict_child_data.WriteStringRef(layout.FetchStringFromDict(offsets[i], str_len));
	}
}

unsafe_array_ptr<const uint8_t> CompressedStringScanState::GetSelectionBytes(idx_t start,
                                                                             idx_t decompress_count) const {
	const auto group_size = BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
	const auto group_bytes = group_size * layout.current_width / 8;
	D_ASSERT(decompress_count % group_size == 0);
	D_ASSERT(group_bytes == 0 || start / group_size <= layout.selection_reader.Size() / group_bytes);
	// Start at the group containing the requested row.
	// Divide before multiplying to avoid overflowing start * current_width.
	const auto source_offset = (start / group_size) * group_bytes;
	D_ASSERT(group_bytes == 0 ||
	         decompress_count / group_size <= (layout.selection_reader.Size() - source_offset) / group_bytes);
	const auto source_size = BitpackingPrimitives::GetRequiredSize(decompress_count, layout.current_width);
	return layout.selection_reader.GetBytes(source_offset, source_size);
}

template <bool NEEDS_STRING_OFFSET_CHECK>
void CompressedStringScanState::ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count) {
	// Handling non-bitpacking-group-aligned start values;
	idx_t start_offset = start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	// We will scan in blocks of BITPACKING_ALGORITHM_GROUP_SIZE, so we may scan some extra values.
	idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count + start_offset);
	const auto source = GetSelectionBytes(start, decompress_count);

	// Create a decompression buffer of sufficient size if we don't already have one.
	if (!sel_vec || sel_vec_size < decompress_count) {
		sel_vec_size = decompress_count;
		sel_vec = make_buffer<SelectionVector>(decompress_count);
	}

	sel_t *sel_vec_ptr = sel_vec->data();

	BitpackingPrimitives::UnPackBuffer<sel_t>(data_ptr_cast(sel_vec_ptr), source.data(), decompress_count,
	                                          layout.current_width);

	auto result_data = FlatVector::Writer<string_t>(result, scan_count, result_offset);

	const auto &offsets = layout.index_buffer;
	bool has_error = false;
	for (idx_t i = 0; i < scan_count; i++) {
		// Lookup dict offset in index buffer
		auto string_dict_index = sel_vec->get_index(i + start_offset);

		if (NEEDS_STRING_OFFSET_CHECK) {
			const auto val = layout.ValidateAndGetEntry(string_dict_index);
			result_data.WriteStringRef(val);
			continue;
		}

		bool elem_error = string_dict_index >= offsets.size();
		string_dict_index = elem_error ? 0 : string_dict_index;
		auto str_dict_offset = offsets[string_dict_index];

		has_error |= elem_error;

		const auto str_len = layout.GetStringLength(string_dict_index);
		result_data.WriteStringRef(layout.FetchStringFromDict(str_dict_offset, str_len));
	}

	if (has_error) {
		ThrowDictionaryIndexOutOfRange();
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
	auto source = GetSelectionBytes(start, decompress_count);

	// Create a selection vector of sufficient size if we don't already have one.
	if (!sel_vec || sel_vec_size < decompress_count) {
		sel_vec_size = decompress_count;
		sel_vec = make_buffer<SelectionVector>(decompress_count);
	}

	// Scanning 2048 values, emitting a dict vector
	data_ptr_t dst = data_ptr_cast(sel_vec->data());

	BitpackingPrimitives::UnPackBuffer<sel_t>(dst, source.data(), decompress_count, layout.current_width);

	sel_vec->ShiftLeft(start_offset, scan_count);
	layout.ValidateDictionary(*sel_vec, scan_count);

	result.Dictionary(dictionary, *sel_vec, scan_count);
}

} // namespace duckdb
