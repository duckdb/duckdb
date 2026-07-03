//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/standard_compression_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/statistics/stats_writer.hpp"

namespace duckdb {

struct StandardCompressionState : public CompressionState {
	explicit StandardCompressionState(ColumnDataCheckpointData &checkpoint_data, CompressionType compression_type)
	    : CompressionState(checkpoint_data, compression_type) {
	}
	~StandardCompressionState() override;

	void CreateAndPinNewSegment();
	void FlushCurrentSegment(idx_t segment_size);
	template <class T>
	void FlushCurrentSegment(T &stats_writer, idx_t segment_size) {
		// merge the stats into the segment stats
		stats_writer.Merge(current_segment->GetStatsMutable());
		// flush the segment
		FlushCurrentSegment(segment_size);

		// clear the stats writer for the next segment
		stats_writer.Clear();
	}

public:
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;
};

} // namespace duckdb
