#pragma once

#include "duckdb/storage/compression/dictionary/common.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
// FIXME: why is this StringScanState when we also define: `BufferHandle handle` ???
struct CompressedStringScanState : public StringScanState {
public:
	explicit CompressedStringScanState(BufferHandle &&handle_p)
	    : StringScanState(), owned_handle(std::move(handle_p)), handle(owned_handle) {
	}
	explicit CompressedStringScanState(BufferHandle &handle_p) : StringScanState(), owned_handle(), handle(handle_p) {
	}

public:
	void Initialize(ColumnSegment &segment, bool initialize_dictionary = true);
	void ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count);
	void ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset, idx_t start,
	                            idx_t scan_count);

private:
	string_t FetchStringFromDict(int32_t dict_offset, uint16_t string_len);
	uint16_t GetStringLength(sel_t index);

public:
	BufferHandle owned_handle;
	optional_ptr<BufferHandle> handle;

	bitpacking_width_t current_width;
	buffer_ptr<SelectionVector> sel_vec;
	idx_t sel_vec_size = 0;

	//! Start of the block (pointing to the dictionary_header)
	data_ptr_t baseptr;
	//! Start of the data (pointing to the start of the selection buffer)
	data_ptr_t base_data;
	uint32_t *index_buffer_ptr;
	uint32_t index_buffer_count;

	buffer_ptr<Vector> dictionary;
	idx_t dictionary_size;
	StringDictionaryContainer dict;
	idx_t block_size;
};

} // namespace duckdb
