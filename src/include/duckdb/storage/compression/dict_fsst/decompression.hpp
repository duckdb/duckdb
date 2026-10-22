#pragma once

#include "duckdb/common/bitpacking.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/compression/compression_segment_reader.hpp"
#include "duckdb/storage/compression/dict_fsst/common.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

class DictionaryEntry;

namespace dict_fsst {

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct CompressedStringScanState : public SegmentScanState {
private:
	struct SegmentLayout {
		//! Compression mode read from disk, validated as mode < DictFSSTMode::COUNT.
		DictFSSTMode mode;
		//! Dictionary entry count read from disk, validated as nonzero.
		//! In FSST_ONLY mode, dict_count - 1 is at least the segment row count.
		//! With zero-width lengths, at most the NULL and empty-string entries are present.
		uint32_t dict_count;
		//! Bit widths read from disk, both validated as at most 32.
		bitpacking_width_t dictionary_indices_width;
		//! String lengths have a nonzero width in FSST modes.
		bitpacking_width_t string_lengths_width;
		//! Dictionary string bytes, FSST-encoded unless mode is DICTIONARY.
		CompressionSegmentReader dictionary;
		//! Serialized FSST symbol table.
		CompressionSegmentReader symbol_table;
		//! Packed per-entry string lengths from disk, decoded lengths are unvalidated.
		unsafe_array_ptr<const uint8_t> string_lengths;
		//! Packed row-to-dictionary indices from disk, decoded indices are unvalidated.
		unsafe_array_ptr<const uint8_t> dictionary_indices;
	};

public:
	CompressedStringScanState(ColumnSegment &segment, BufferHandle &&handle_p)
	    : segment(segment), owned_handle(std::move(handle_p)), layout(ReadLayout(owned_handle, segment)) {
	}
	CompressedStringScanState(ColumnSegment &segment, BufferHandle &handle_p)
	    : segment(segment), owned_handle(), layout(ReadLayout(handle_p, segment)) {
	}

	~CompressedStringScanState() override;

public:
	void Initialize(bool initialize_dictionary = true);
	void ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count);
	void ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset, idx_t start,
	                            idx_t scan_count);
	const SelectionVector &GetSelVec(idx_t start, idx_t scan_count);
	void Select(Vector &result, idx_t result_offset, idx_t start, const SelectionVector &sel, idx_t sel_count);

	bool AllowDictionaryScan(idx_t scan_count);

private:
	static SegmentLayout ReadLayout(const BufferHandle &handle, const ColumnSegment &segment);
	string_t FetchStringFromDict(Vector &result, uint32_t dict_offset, idx_t dict_idx);

public:
	ColumnSegment &segment;
	BufferHandle owned_handle;
	SegmentLayout layout;

	buffer_ptr<SelectionVector> sel_vec;
	idx_t sel_vec_size = 0;

	// decompress offset/position - used for scanning without a dictionary
	uint32_t decompress_offset = 0;
	idx_t decompress_position = 0;

	//! Decoded lengths for layout.dict_count entries, followed by unpacking padding.
	//! The NULL length is zero and the total length does not exceed the dictionary size.
	vector<uint32_t> string_lengths;

	buffer_ptr<DictionaryEntry> dictionary;
	void *decoder = nullptr;
	bool all_values_inlined = false;

	unsafe_unique_array<bool> filter_result;
	bool null_filter_result_initialized = false;
};

} // namespace dict_fsst

} // namespace duckdb
