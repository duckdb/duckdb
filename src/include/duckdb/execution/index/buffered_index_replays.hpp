//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/buffered_index_replays.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ColumnDataCollection;

enum class BufferedIndexReplay : uint8_t { INSERT_ENTRY = 0, DEL_ENTRY = 1 };

struct ReplayRange {
	BufferedIndexReplay type;
	// [start, end) - start is inclusive, end is exclusive for the range within the ColumnDataCollection
	// buffer for operations to replay for this range.
	idx_t start;
	idx_t end;
	explicit ReplayRange(const BufferedIndexReplay replay_type, const idx_t start_p, const idx_t end_p)
	    : type(replay_type), start(start_p), end(end_p) {
	}
};

// All inserts and deletes to be replayed are stored in their respective buffers.
// Since the inserts and deletes may be interleaved, however, ranges stores the ordering of operations
// and their offsets in the respective buffer.
// Simple example:
// ranges[0] - INSERT_ENTRY, [0,6)
// ranges[1] - DEL_ENTRY,    [0,3)
// ranges[2] - INSERT_ENTRY  [6,12)
// So even though the buffered_inserts has all the insert data from [0,12), ranges gives us the intervals for
// replaying the index operations in the right order.
struct BufferedIndexReplays {
	vector<ReplayRange> ranges;
	unique_ptr<ColumnDataCollection> buffered_inserts;
	unique_ptr<ColumnDataCollection> buffered_deletes;

	BufferedIndexReplays() = default;

	unique_ptr<ColumnDataCollection> &GetBuffer(const BufferedIndexReplay replay_type) {
		if (replay_type == BufferedIndexReplay::INSERT_ENTRY) {
			return buffered_inserts;
		}
		return buffered_deletes;
	}

	bool HasBufferedReplays() const {
		return !ranges.empty();
	}
};

} // namespace duckdb

