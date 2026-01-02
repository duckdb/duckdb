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
	    : segment(segment), owned_handle(std::move(handle_p)), handle(owned_handle) {
	}
	CompressedStringScanState(ColumnSegment &segment, BufferHandle &handle_p)
	    : segment(segment), owned_handle(), handle(handle_p) {
	}

	~CompressedStringScanState() override;

public:
	void Initialize(bool initialize_dictionary = true);
	void ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count);
	void ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset, idx_t start,
	                            idx_t scan_count);
	const SelectionVector &GetSelVec(idx_t start, idx_t scan_count);
	void Select(Vector &result, idx_t start, const SelectionVector &sel, idx_t sel_count);

	bool AllowDictionaryScan(idx_t scan_count);

private:
	string_t FetchStringFromDict(Vector &result, uint32_t dict_offset, idx_t dict_idx);

public:
	ColumnSegment &segment;
	BufferHandle owned_handle;
	optional_ptr<BufferHandle> handle;

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

	//! Start of the block (pointing to the dictionary_header)
	data_ptr_t baseptr;
	data_ptr_t dict_ptr;
	data_ptr_t dictionary_indices_ptr;
	data_ptr_t string_lengths_ptr;

	buffer_ptr<VectorChildBuffer> dictionary;
	void *decoder = nullptr;
	bool all_values_inlined = false;

	unsafe_unique_array<bool> filter_result;
};

} // namespace dict_fsst

} // namespace duckdb
