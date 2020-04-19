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
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/uncompressed_segment.hpp"

namespace duckdb {

class PersistentSegment : public ColumnSegment {
public:
	PersistentSegment(BufferManager &manager, block_id_t id, idx_t offset, TypeId type, idx_t start, idx_t count,
	                  data_t stats_min[], data_t stats_max[]);

	//! The buffer manager
	BufferManager &manager;
	//! The block id that this segment relates to
	block_id_t block_id;
	//! The offset into the block
	idx_t offset;
	//! The uncompressed segment that the data of the persistent segment is loaded into
	unique_ptr<UncompressedSegment> data;

public:
	void InitializeScan(ColumnScanState &state) override;
	//! Scan one vector from this transient segment
	void Scan(Transaction &transaction, ColumnScanState &state, idx_t vector_index, Vector &result) override;
	//! Scan one vector from this transient segment, throwing an exception if there are any outstanding updates
	void IndexScan(ColumnScanState &state, Vector &result) override;
	//! Scan the next vector from the column and apply a selection vector to filter the data
	void FilterScan(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
	                idx_t &approved_tuple_count) override;
	//! Executes the filters directly in the table's data
	void Select(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
	            idx_t &approved_tuple_count, vector<TableFilter> &tableFilter) override;
	//! Fetch the base table vector index that belongs to this row
	void Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) override;
	//! Fetch a value of the specific row id and append it to the result
	void FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result,
	              idx_t result_idx) override;

	//! Perform an update within the segment
	void Update(ColumnData &column_data, Transaction &transaction, Vector &updates, row_t *ids, idx_t count) override;
};

} // namespace duckdb
