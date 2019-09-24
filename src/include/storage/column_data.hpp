//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "storage/table/append_state.hpp"
#include "storage/table/scan_state.hpp"
#include "storage/table/persistent_segment.hpp"

namespace duckdb {
class DataTable;
class PersistentSegment;
class Transaction;

class ColumnData {
public:
	ColumnData();
	//! Set up the column data with the set of persistent segments, returns the amount of rows
	void Initialize(vector<unique_ptr<PersistentSegment>>& segments);

	//! The type of the column
	TypeId type;
	//! The table of the column
	DataTable *table;
	//! The persistent data of the column
	SegmentTree persistent;
	//! The transient data of the column
	SegmentTree transient;
	//! The amount of persistent rows
	index_t persistent_rows;
public:
	//! Initialize a scan of the column
	void InitializeTransientScan(TransientScanState &state);
	//! Scan the next vector from the transient part of the column
	void TransientScan(Transaction &transaction, TransientScanState &state, Vector &result);
	//! Skip a single vector in the transient scan, moving on to the next one
	void SkipTransientScan(TransientScanState &state);

	//! Initialize an appending phase to this column
	void InitializeAppend(ColumnAppendState &state);
	//! Append a vector of type [type] to the end of the column
	void Append(ColumnAppendState &state, Vector &vector);

	//! Append a transient segment
	void AppendTransientSegment(index_t start_row);
};

}
