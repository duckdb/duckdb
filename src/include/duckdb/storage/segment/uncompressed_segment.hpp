//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/segment/uncompressed_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/segment/base_segment.hpp"

namespace duckdb {

//! An uncompressed segment represents an uncompressed segment of a column residing in a block
class UncompressedSegment : public BaseSegment {
public:
	UncompressedSegment(DatabaseInstance &db, PhysicalType type, idx_t row_start);
	virtual ~UncompressedSegment();

public:
	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats
	//! in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is
	//! full.
	virtual idx_t Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) = 0;
	//! Truncate a previous append
	virtual void RevertAppend(idx_t start_row);

	bool IsUncompressed() override {
		return true;
	}
};

} // namespace duckdb
