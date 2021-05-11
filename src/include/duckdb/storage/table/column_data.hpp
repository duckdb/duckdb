//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"

namespace duckdb {
class ColumnData;
class DatabaseInstance;
class RowGroup;
class TableDataWriter;
class PersistentSegment;
class PersistentColumnData;
class Transaction;

struct DataTableInfo;

class ColumnData {
public:
	ColumnData(DatabaseInstance &db, idx_t start_row, LogicalType type, ColumnData *parent);
	virtual ~ColumnData();

	//! The database instance this column belongs to
	DatabaseInstance &db;
	//! The start row
	idx_t start;
	//! The type of the column
	LogicalType type;
	//! The parent column (if any)
	ColumnData *parent;

public:
	virtual bool CheckZonemap(ColumnScanState &state, TableFilter &filter) = 0;

	DatabaseInstance &GetDatabase() const;

	//! The root type of the column
	const LogicalType &RootType() const;

	void ScanVector(ColumnScanState &state, Vector &result);

	//! Initialize a scan of the column
	virtual void InitializeScan(ColumnScanState &state) = 0;
	//! Initialize a scan starting at the specified offset
	virtual void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) = 0;
	//! Scan the next vector from the column
	virtual void Scan(ColumnScanState &state, Vector &result) = 0;
	//! Scan the next vector from the column and apply a selection vector to filter the data
	void FilterScan(ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t &approved_tuple_count);
	//! Executes the filters directly in the table's data
	void Select(ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t &approved_tuple_count,
	            vector<TableFilter> &table_filter);
	//! Initialize an appending phase for this column
	virtual void InitializeAppend(ColumnAppendState &state);
	//! Append a vector of type [type] to the end of the column
	void Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count);
	virtual void AppendData(BaseStatistics &stats, ColumnAppendState &state, VectorData &vdata, idx_t count);
	//! Revert a set of appends to the ColumnData
	virtual void RevertAppend(row_t start_row);

	//! Fetch the vector from the column data that belongs to this specific row
	virtual void Fetch(ColumnScanState &state, row_t row_id, Vector &result);
	//! Fetch a specific row id and append it to the vector
	virtual void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx);

	virtual void CommitDropColumn();

	virtual unique_ptr<ColumnCheckpointState> CreateCheckpointState(RowGroup &row_group, TableDataWriter &writer);
	virtual unique_ptr<ColumnCheckpointState> Checkpoint(RowGroup &row_group, TableDataWriter &writer,
	                                                     idx_t column_idx);

	virtual void Initialize(PersistentColumnData &column_data);

	static void BaseDeserialize(DatabaseInstance &db, Deserializer &source, const LogicalType &type,
	                            ColumnData &result);
	static shared_ptr<ColumnData> Deserialize(DatabaseInstance &db, idx_t start_row, Deserializer &source,
	                                          const LogicalType &type);

protected:
	//! Append a transient segment
	void AppendTransientSegment(idx_t start_row);

protected:
	//! The segments holding the data of this column segment
	SegmentTree data;
};

} // namespace duckdb
