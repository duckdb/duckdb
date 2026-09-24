#pragma once

#include "duckdb/storage/compression/compression_segment_reader.hpp"
#include "duckdb/storage/compression/dictionary/common.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct CompressedStringScanState : public SegmentScanState {
private:
	//! Dictionary segment data from disk, with byte ranges checked by ReadLayout.
	struct DictionarySegmentLayout {
		void ValidateDictionary(const SelectionVector &sel, idx_t scan_count) const;
		//! Validate the index buffer (offsets monotonic and within the dictionary) so scans can trust it.
		void ValidateIndexBuffer() const;
		//! Validate the dictionary index and its offsets before reading the string.
		string_t ValidateAndGetEntry(idx_t index) const;
		//! The index must be within the table, and the offsets must be nondecreasing to avoid underflow.
		uint32_t GetStringLength(sel_t index) const;
		//! The offset must be within the dictionary, and the length must not extend past its end.
		string_t FetchStringFromDict(uint32_t dict_offset, uint32_t string_len) const;

		//! Bits per dictionary index, derived from the entry count and checked against the stored width.
		bitpacking_width_t current_width;
		//! Packed indices, mapping each row to a dictionary entry.
		CompressionSegmentReader selection_reader;
		//! String contents stored backwards from dict_end.
		CompressionSegmentReader dictionary_reader;
		//! Offsets to the strings, measured backwards from dict_end.
		//! Consecutive offsets determine each string's length.
		unsafe_array_ptr<const uint32_t> index_buffer;
	};

public:
	CompressedStringScanState(BufferHandle &&handle_p, const ColumnSegment &segment)
	    : owned_handle(std::move(handle_p)), layout(ReadLayout(owned_handle, segment)) {
	}
	CompressedStringScanState(BufferHandle &handle_p, const ColumnSegment &segment)
	    : layout(ReadLayout(handle_p, segment)) {
	}

public:
	void InitializeDictionary(const ColumnSegment &segment);
	template <bool NEEDS_STRING_OFFSET_CHECK = false>
	void ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count);
	void ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset, idx_t start,
	                            idx_t scan_count);

private:
	static DictionarySegmentLayout ReadLayout(const BufferHandle &handle, const ColumnSegment &segment);
	//! Returns packed bytes starting at the group containing start.
	//! decompress_count must cover whole bitpacking groups that fit within the selection stream.
	unsafe_array_ptr<const uint8_t> GetSelectionBytes(idx_t start, idx_t decompress_count) const;

public:
	BufferHandle owned_handle;

	DictionarySegmentLayout layout;
	buffer_ptr<SelectionVector> sel_vec;
	idx_t sel_vec_size = 0;

	buffer_ptr<DictionaryEntry> dictionary;
};

} // namespace duckdb
