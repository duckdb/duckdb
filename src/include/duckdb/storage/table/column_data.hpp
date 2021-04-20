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

namespace duckdb {
class ColumnData;
class DatabaseInstance;
class Morsel;
class TableDataWriter;
class PersistentSegment;
class PersistentColumnData;
class Transaction;

struct DataTableInfo;

struct ColumnCheckpointState {
	ColumnCheckpointState(ColumnData &column_data, TableDataWriter &writer);
	virtual ~ColumnCheckpointState();

	ColumnData &column_data;
	TableDataWriter &writer;
	SegmentTree new_tree;
	vector<DataPointer> data_pointers;
	unique_ptr<BaseStatistics> global_stats;

	unique_ptr<UncompressedSegment> current_segment;
	unique_ptr<SegmentStatistics> segment_stats;

public:
	virtual void CreateEmptySegment();
	virtual void FlushSegment();
	virtual void AppendData(Vector &data, idx_t count);
	virtual void FlushToDisk();
};

class ColumnData {
public:
	ColumnData(Morsel &morsel, LogicalType type, idx_t column_idx, ColumnData *parent);
	virtual ~ColumnData();

	//! The morsel this column chunk belongs to
	Morsel &morsel;
	//! The type of the column
	LogicalType type;
	//! The column index of the column
	idx_t column_idx;
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
	void FilterScan(ColumnScanState &state, Vector &result, SelectionVector &sel,
	                idx_t &approved_tuple_count);
	//! Executes the filters directly in the table's data
	void Select(ColumnScanState &state, Vector &result, SelectionVector &sel,
	            idx_t &approved_tuple_count, vector<TableFilter> &table_filter);
	//! Initialize an appending phase for this column
	virtual void InitializeAppend(ColumnAppendState &state);
	//! Append a vector of type [type] to the end of the column
	void Append(ColumnAppendState &state, Vector &vector, idx_t count);
	virtual void AppendData(ColumnAppendState &state, VectorData &vdata, idx_t count);
	//! Revert a set of appends to the ColumnData
	virtual void RevertAppend(row_t start_row);

	//! Fetch the vector from the column data that belongs to this specific row
	virtual void Fetch(ColumnScanState &state, row_t row_id, Vector &result);
	//! Fetch a specific row id and append it to the vector
	virtual void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx);

	void SetStatistics(unique_ptr<BaseStatistics> new_stats);
	void MergeStatistics(BaseStatistics &other);
	virtual unique_ptr<BaseStatistics> GetStatistics();

	virtual void CommitDropColumn();

	virtual unique_ptr<ColumnCheckpointState> CreateCheckpointState(TableDataWriter &writer);
	virtual void Checkpoint(TableDataWriter &writer);

	virtual void Initialize(PersistentColumnData &column_data);

	static void BaseDeserialize(DatabaseInstance &db, Deserializer &source, const LogicalType &type,
	                            PersistentColumnData &result);
	static unique_ptr<PersistentColumnData> Deserialize(DatabaseInstance &db, Deserializer &source,
	                                                    const LogicalType &type);

protected:
	//! Append a transient segment
	void AppendTransientSegment(idx_t start_row);

protected:
	mutex stats_lock;
	//! The segments holding the data of this column segment
	SegmentTree data;
	//! The statistics of the column
	unique_ptr<BaseStatistics> statistics;
};

} // namespace duckdb
