//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "storage/table/persistent_segment.hpp"

namespace duckdb {
class Transaction;

struct ColumnScanState {
	ColumnPointer pointer;
};

class ColumnData {
public:
	ColumnData(TypeId type, vector<unique_ptr<PersistentSegment>>& segments);

	//! The type of the column
	TypeId type;
	//! The data of the column
	SegmentTree data;
public:
	//! Initialize a scan of the column
	void InitializeScan(ColumnScanState &state);
	//! Scan the next STANDARD_VECTOR_SIZE entries from the column, and place them in "result"
	void Scan(Transaction &transaction, Vector &result, ColumnScanState &state);
	//! Append a vector of type [type] to the end of the column
	void Append(Vector &vector);
};

}
