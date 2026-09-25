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
	struct SegmentLayout {
		//! Check each selected dictionary index against the offset table.
		//! The selection must be set and the requested range must fit within its capacity.
		void ValidateDictionaryIndices(const SelectionVector &sel, idx_t start_offset, idx_t scan_count) const;
		//! Validate the index buffer (offsets monotonic and within the dictionary) so scans can trust it.
		void ValidateIndexBuffer() const;
		//! Validate the dictionary index and its offsets before reading the string.
		string_t ValidateAndGetEntry(idx_t index) const;
		//! The index must be within the table and the offsets must be nondecreasing to avoid underflow.
		uint32_t GetStringLength(idx_t index) const;
		//! The offset must be within the dictionary and the length must not extend past its end.
		string_t FetchStringFromDict(uint32_t dict_offset, uint32_t string_len) const;

		//! Bits per dictionary index, derived from the entry count and checked against the stored width.
		bitpacking_width_t current_width;
		//! Packed indices, mapping each row to a dictionary entry.
		CompressionSegmentReader selection_reader;
		//! String contents stored backwards from dict_end.
		CompressionSegmentReader dictionary_reader;
		//! Maps dictionary indices to byte offsets measured backwards from dict_end.
		//! Consecutive offsets determine each (non NULL) string's length.
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
	//! Requires a materialized dictionary.
	void ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count);
	void ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset, idx_t start,
	                            idx_t scan_count);
	//! result must be flat and have capacity for the entry at result_offset.
	void FetchRow(Vector &result, idx_t result_offset, idx_t row_id);

private:
	static SegmentLayout ReadLayout(const BufferHandle &handle, const ColumnSegment &segment);
	//! Returns packed bytes starting at the group containing start.
	//! decompress_count must cover whole bitpacking groups that fit within the selection stream.
	unsafe_array_ptr<const uint8_t> GetSelectionBytes(idx_t start, idx_t decompress_count) const;
	//! Unpack into sel_vec, starting with the group containing start.
	//! decompress_count must cover whole bitpacking groups that fit within the selection stream.
	void UnpackSelection(idx_t start, idx_t decompress_count);

public:
	BufferHandle owned_handle;

	SegmentLayout layout;
	buffer_ptr<SelectionVector> sel_vec;
	idx_t sel_vec_size = 0;

	buffer_ptr<DictionaryEntry> dictionary;
};

} // namespace duckdb
