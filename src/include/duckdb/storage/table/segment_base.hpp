//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/segment_base.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

class SegmentBase {
public:
	SegmentBase(idx_t start, idx_t count) : start(start), count(count) {
	}
	virtual ~SegmentBase() {
	}

	//! The start row id of this chunk
	idx_t start;
	//! The amount of entries in this storage chunk
	idx_t count;
	//! The next segment after this one
	unique_ptr<SegmentBase> next;
};

} // namespace duckdb
