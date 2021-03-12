//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/validity_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/block.hpp"
#include "duckdb/storage/table/segment_tree.hpp"
#include "duckdb/storage/table/segment_base.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {
class BlockHandle;
class DatabaseInstance;
class SegmentStatistics;
class Vector;
struct VectorData;

class ValiditySegment : public SegmentBase {
public:
	ValiditySegment(DatabaseInstance &db, idx_t start, idx_t count, block_id_t block_id = INVALID_BLOCK);
	~ValiditySegment();

	DatabaseInstance &db;
	//! The block that this segment relates to
	shared_ptr<BlockHandle> block;
	//! The maximum amount of vectors that can be stored in this segment
	idx_t max_vector_count;
public:
	void Fetch(idx_t vector_index, ValidityMask &result);

	idx_t Append(VectorData &data, idx_t offset, idx_t count);

	bool IsValid(idx_t row_index);

};


} // namespace duckdb
