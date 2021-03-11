//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/validity_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/segment_base.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {
class BlockHandle;
class SegmentStatistics;
class Vector;

class ValiditySegment : public SegmentBase {
public:
	ValiditySegment(idx_t start, idx_t count);
	~ValiditySegment();

	//! Whether or not the block has ANY valid (i.e. non-null) values
	bool has_valid_values;
	//! Whether or not the block has ANY invalid (i.e. null) values
	bool has_invalid_values;
	//! The block that this segment relates to
	shared_ptr<BlockHandle> block;
	//! The maximum amount of vectors that can be stored in this segment
	idx_t max_vector_count;
public:
	void Fetch(idx_t vector_index, ValidityMask &result);

	idx_t Append(SegmentStatistics &stats, Vector &data, idx_t offset, idx_t count);

};


} // namespace duckdb
