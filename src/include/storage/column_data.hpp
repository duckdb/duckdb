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
	//! Set up the column data with the set of persistent segments, returns the amount of rows
	index_t Initialize(vector<unique_ptr<PersistentSegment>>& segments);

	//! The type of the column
	TypeId type;
	//! The table of the column
	DataTable *table;
	//! The data of the column
	SegmentTree data;
public:
	//! Initialize a scan of the column
	void InitializeScan(ColumnScanState &state);
	//! Scan the next STANDARD_VECTOR_SIZE entries from the column, and place them in "result"
	void Scan(Transaction &transaction, Vector &result, ColumnScanState &state, index_t count);

	//! Initialize an appending phase to this column
	void InitializeAppend(ColumnAppendState &state);
	//! Append a vector of type [type] to the end of the column
	void Append(ColumnAppendState &state, Vector &vector);

	//! Append a transient segment
	void AppendTransientSegment(index_t start_row);
};

}
