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

class SegmentBase {
public:
	SegmentBase(idx_t start, idx_t count) : start(start), count(count), next(nullptr) {
	}
	virtual ~SegmentBase() {
	}
	SegmentBase *Next() {
#ifndef DUCKDB_R_BUILD
		return next.load();
#else
		return next;
#endif
	}

	//! The start row id of this chunk
	const idx_t start;
	//! The amount of entries in this storage chunk
	atomic<idx_t> count;
	//! The next segment after this one

#ifndef DUCKDB_R_BUILD
	atomic<SegmentBase *> next;
#else
	SegmentBase *next;
#endif
};

} // namespace duckdb
