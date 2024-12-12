#pragma once

#include "duckdb/storage/compression/dictionary/common.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
// FIXME: why is this StringScanState when we also define: `BufferHandle handle` ???
struct CompressedStringScanState : public StringScanState {
public:
	CompressedStringScanState() : StringScanState() {
	}

public:
	BufferHandle handle;
	buffer_ptr<Vector> dictionary;
	idx_t dictionary_size;
	bitpacking_width_t current_width;
	buffer_ptr<SelectionVector> sel_vec;
	idx_t sel_vec_size = 0;
};

} // namespace duckdb
