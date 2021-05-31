//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/persistent_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/block.hpp"

namespace duckdb {
class DatabaseInstance;

class PersistentSegment : public ColumnSegment {
public:
	PersistentSegment(DatabaseInstance &db, block_id_t id, idx_t offset, const LogicalType &type, idx_t start,
	                  idx_t count, unique_ptr<BaseStatistics> statistics);

	//! The block id that this segment relates to
	block_id_t block_id;
	//! The offset into the block
	idx_t offset;
};

} // namespace duckdb
