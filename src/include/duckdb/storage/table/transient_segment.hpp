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
#include "duckdb/storage/uncompressed_segment.hpp"

namespace duckdb {
struct ColumnAppendState;
class DatabaseInstance;
class PersistentSegment;

class TransientSegment : public ColumnSegment {
public:
	TransientSegment(DatabaseInstance &db, const LogicalType &type, idx_t start);
	TransientSegment(PersistentSegment &segment);

	//! The storage manager
	DatabaseInstance &db;
	//! The uncompressed segment holding the data
	unique_ptr<UncompressedSegment> data;

public:
	void InitializeScan(ColumnScanState &state) override;
	//! Scan one vector from this transient segment
	void Scan(ColumnScanState &state, idx_t vector_index, Vector &result) override;
	//! Fetch the base table vector index that belongs to this row
	void Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) override;
	//! Fetch a value of the specific row id and append it to the result
	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) override;

	//! Initialize an append of this transient segment
	void InitializeAppend(ColumnAppendState &state);
	//! Appends a (part of) vector to the transient segment, returns the amount of entries successfully appended
	idx_t Append(ColumnAppendState &state, VectorData &data, idx_t offset, idx_t count);
	//! Revert an append made to this transient segment
	void RevertAppend(idx_t start_row);
};

} // namespace duckdb
