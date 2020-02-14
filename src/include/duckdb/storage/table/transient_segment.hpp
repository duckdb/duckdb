//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/transient_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/uncompressed_segment.hpp"

namespace duckdb {
struct ColumnAppendState;

class TransientSegment : public ColumnSegment {
public:
	TransientSegment(BufferManager &manager, TypeId type, index_t start);

	//! The buffer manager
	BufferManager &manager;
	//! The uncompressed segment holding the data
	unique_ptr<UncompressedSegment> data;

public:
	void InitializeScan(ColumnScanState &state) override;
	//! Scan one vector from this transient segment
	void Scan(Transaction &transaction, ColumnScanState &state, index_t vector_index, Vector &result) override;
	//! Scan one vector from this transient segment, throwing an exception if there are any outstanding updates
	void IndexScan(ColumnScanState &state, Vector &result) override;

	//! Fetch the base table vector index that belongs to this row
	void Fetch(ColumnScanState &state, index_t vector_index, Vector &result) override;
	//! Fetch a value of the specific row id and append it to the result
	void FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result, index_t result_idx) override;

	//! Perform an update within the transient segment
	void Update(ColumnData &column_data, Transaction &transaction, Vector &updates, row_t *ids) override;

	//! Initialize an append of this transient segment
	void InitializeAppend(ColumnAppendState &state);
	//! Appends a (part of) vector to the transient segment, returns the amount of entries successfully appended
	index_t Append(ColumnAppendState &state, Vector &data, index_t offset, index_t count);
	//! Revert an append made to this transient segment
	void RevertAppend(index_t start_row);
};

} // namespace duckdb
