#pragma once

#include "duckdb/storage/compression/dict_fsst/common.hpp"

namespace duckdb {

namespace dict_fsst {

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct CompressedStringScanState : public SegmentScanState {
public:
	CompressedStringScanState(ColumnSegment &segment, BufferHandle &&handle_p)
	    : segment(segment), handle(std::move(handle_p)) {
	}

	~CompressedStringScanState() override;

public:
	void Initialize(bool initialize_dictionary = true);
	void ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count,
	                      StringScanType string_scan_type = StringScanType::STANDARD);
	void ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset, idx_t start,
	                            idx_t scan_count);
	const SelectionVector &GetSelVec(idx_t start, idx_t scan_count);
	void Select(Vector &result, idx_t result_offset, idx_t start, const SelectionVector &sel, idx_t sel_count,
	            StringScanType string_scan_type = StringScanType::STANDARD);

	bool AllowDictionaryScan(idx_t scan_count);

private:
	//! With COPY_STRINGS the string is owned by the result instead of pointing into the block
	string_t FetchStringFromDict(Vector &result, uint32_t dict_offset, idx_t dict_idx,
	                             StringScanType string_scan_type = StringScanType::STANDARD);
	//! Builds the dictionary on first use, returns false if this scan has no dictionary
	bool EnsureDictionary();
	//! The offset of each string in the dictionary, computed on first use
	const vector<uint32_t> &GetStringOffsets();

public:
	ColumnSegment &segment;
	BufferHandle handle;

	DictFSSTMode mode;
	idx_t dictionary_size;
	uint32_t dict_count;
	bitpacking_width_t dictionary_indices_width;
	bitpacking_width_t string_lengths_width;

	buffer_ptr<SelectionVector> sel_vec;
	idx_t sel_vec_size = 0;

	// decompress offset/position - used for scanning without a dictionary
	uint32_t decompress_offset = 0;
	idx_t decompress_position = 0;

	vector<uint32_t> string_lengths;
	vector<uint32_t> string_offsets;

	//! Start of the block (pointing to the dictionary_header)
	data_ptr_t baseptr;
	data_ptr_t dict_ptr;
	data_ptr_t dictionary_indices_ptr;
	data_ptr_t string_lengths_ptr;

	buffer_ptr<DictionaryEntry> dictionary;
	bool build_dictionary = false;
	void *decoder = nullptr;
	bool all_values_inlined = false;

	unsafe_unique_array<bool> filter_result;
	bool null_filter_result_initialized = false;
};

} // namespace dict_fsst

} // namespace duckdb
