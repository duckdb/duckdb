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

template <class T>
class SegmentBase {
public:
	SegmentBase(idx_t start, idx_t count) : start(start), count(count) {
	}

	idx_t GetSegmentStart() const {
		return start;
	}
	void SetSegmentStart(idx_t start_p) {
		this->start = start_p;
	}

	//! The amount of entries in this storage chunk
	atomic<idx_t> count;

private:
	//! The start row id of this chunk
	idx_t start;
};

} // namespace duckdb
