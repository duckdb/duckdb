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
#include "duckdb/storage/uncompressed_segment.hpp"

namespace duckdb {
class DatabaseInstance;

class PersistentSegment : public ColumnSegment {
public:
	PersistentSegment(DatabaseInstance &db, block_id_t id, idx_t offset, const LogicalType &type, idx_t start,
	                  idx_t count, unique_ptr<BaseStatistics> statistics);

	//! The storage manager
	DatabaseInstance &db;
	//! The block id that this segment relates to
	block_id_t block_id;
	//! The offset into the block
	idx_t offset;
	//! The uncompressed segment that the data of the persistent segment is loaded into
	unique_ptr<UncompressedSegment> data;

public:
	void InitializeScan(ColumnScanState &state) override;
	//! Scan one vector from this persistent segment
	void Scan(ColumnScanState &state, idx_t vector_index, Vector &result) override;
	//! Fetch the base table vector index that belongs to this row
	void Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) override;
	//! Fetch a value of the specific row id and append it to the result
	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) override;
};

} // namespace duckdb
