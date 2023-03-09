//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/segment_base.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {
class SegmentTree;

class SegmentBase {
public:
	SegmentBase(idx_t start, idx_t count) : start(start), count(count) {
	}
	virtual ~SegmentBase() {
	}

	//! The index within the segment tree
	idx_t index;
	//! The start row id of this chunk
	const idx_t start;
	//! The amount of entries in this storage chunk
	atomic<idx_t> count;
};

} // namespace duckdb
