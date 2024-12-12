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
	void ScanToFlatVector(ColumnSegment &segment, Vector &result, idx_t result_offset, idx_t start, idx_t scan_count);
	void ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset, idx_t start,
	                            idx_t scan_count);

public:
	BufferHandle owned_handle;
	optional_ptr<BufferHandle> handle;
	buffer_ptr<Vector> dictionary;
	idx_t dictionary_size;
	bitpacking_width_t current_width;
	buffer_ptr<SelectionVector> sel_vec;
	idx_t sel_vec_size = 0;
};

} // namespace duckdb
