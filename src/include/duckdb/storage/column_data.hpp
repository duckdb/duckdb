//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/persistent_segment.hpp"

namespace duckdb {
class PersistentSegment;
class Transaction;

struct DataTableInfo;

class ColumnData {
public:
	ColumnData(BufferManager &manager, DataTableInfo &table_info);
	//! Set up the column data with the set of persistent segments, returns the amount of rows
	void Initialize(vector<unique_ptr<PersistentSegment>> &segments);

	DataTableInfo &table_info;
	//! The type of the column
	TypeId type;
	//! The buffer manager
	BufferManager &manager;
	//! The column index of the column
	idx_t column_idx;
	//! The segments holding the data of the column
	SegmentTree data;
	//! The amount of persistent rows
	idx_t persistent_rows;

public:
	//! Initialize a scan of the column
	void InitializeScan(ColumnScanState &state);
	//! Scan the next vector from the column
	void Scan(Transaction &transaction, ColumnScanState &state, Vector &result);
	//! Scan the next vector from the column and apply a selection vector to filter the data
	void FilterScan(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
	                idx_t &approved_tuple_count);
	//! Scan the next vector from the column, throwing an exception if there are any outstanding updates
	void IndexScan(ColumnScanState &state, Vector &result);
	//! Executes the filters directly in the table's data
	void Select(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
	            idx_t &approved_tuple_count, vector<TableFilter> &tableFilter);
	//! Initialize an appending phase for this column
	void InitializeAppend(ColumnAppendState &state);
	//! Append a vector of type [type] to the end of the column
	void Append(ColumnAppendState &state, Vector &vector, idx_t count);
	//! Revert a set of appends to the ColumnData
	void RevertAppend(row_t start_row);

	//! Update the specified row identifiers
	void Update(Transaction &transaction, Vector &updates, Vector &row_ids, idx_t count);

	//! Fetch the vector from the column data that belongs to this specific row
	void Fetch(ColumnScanState &state, row_t row_id, Vector &result);
	//! Fetch a specific row id and append it to the vector
	void FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result, idx_t result_idx);

private:
	//! Append a transient segment
	void AppendTransientSegment(idx_t start_row);
};

} // namespace duckdb
