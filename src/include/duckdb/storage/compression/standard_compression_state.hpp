//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/standard_compression_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_segment.hpp"

namespace duckdb {

struct StandardCompressionState : public CompressionState {
	explicit StandardCompressionState(ColumnDataCheckpointData &checkpoint_data, CompressionType compression_type)
	    : CompressionState(checkpoint_data, compression_type) {
	}
	~StandardCompressionState() override;

	void CreateAndPinNewSegment();
	void FlushCurrentSegment(idx_t segment_size);

public:
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;
};

} // namespace duckdb
