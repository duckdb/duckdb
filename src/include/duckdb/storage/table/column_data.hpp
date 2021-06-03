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
#include "duckdb/common/mutex.hpp"

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
	ColumnData(DataTableInfo &info, idx_t column_index, idx_t start_row, LogicalType type, ColumnData *parent);
	virtual ~ColumnData();

	//! Table info for the column
	DataTableInfo &info;
	//! The column index of the column, either within the parent table or within the parent
	idx_t column_index;
	//! The start row
	idx_t start;
	//! The type of the column
	LogicalType type;
	//! The parent column (if any)
	ColumnData *parent;

public:
	virtual bool CheckZonemap(ColumnScanState &state, TableFilter &filter) = 0;

	DatabaseInstance &GetDatabase() const;
	DataTableInfo &GetTableInfo() const;

	//! The root type of the column
	const LogicalType &RootType() const;

	//! Initialize a scan of the column
	virtual void InitializeScan(ColumnScanState &state) = 0;
	//! Initialize a scan starting at the specified offset
	virtual void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) = 0;
	//! Scan the next vector from the column
	virtual void Scan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result);
	virtual void ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates);
	virtual void ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t count, Vector &result);

	//! Initialize an appending phase for this column
	virtual void InitializeAppend(ColumnAppendState &state);
	//! Append a vector of type [type] to the end of the column
	virtual void Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count);
	virtual void AppendData(BaseStatistics &stats, ColumnAppendState &state, VectorData &vdata, idx_t count);
	//! Revert a set of appends to the ColumnData
	virtual void RevertAppend(row_t start_row);

	//! Fetch the vector from the column data that belongs to this specific row
	virtual void Fetch(ColumnScanState &state, row_t row_id, Vector &result);
	//! Fetch a specific row id and append it to the vector
	virtual void FetchRow(Transaction &transaction, ColumnFetchState &state, row_t row_id, Vector &result,
	                      idx_t result_idx);

	virtual void Update(Transaction &transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
	                    idx_t update_count);
	virtual void UpdateColumn(Transaction &transaction, const vector<column_t> &column_path, Vector &update_vector,
	                          row_t *row_ids, idx_t update_count, idx_t depth);
	virtual unique_ptr<BaseStatistics> GetUpdateStatistics();

	virtual void CommitDropColumn();

	virtual unique_ptr<ColumnCheckpointState> CreateCheckpointState(RowGroup &row_group, TableDataWriter &writer);
	virtual unique_ptr<ColumnCheckpointState> Checkpoint(RowGroup &row_group, TableDataWriter &writer);

	virtual void CheckpointScan(ColumnSegment *segment, ColumnScanState &state, idx_t row_group_start, idx_t base_row_index, idx_t count, Vector &scan_vector);

	virtual void Initialize(PersistentColumnData &column_data);

	virtual void DeserializeColumn(Deserializer &source);
	static shared_ptr<ColumnData> Deserialize(DataTableInfo &info, idx_t column_index, idx_t start_row,
	                                          Deserializer &source, const LogicalType &type, ColumnData *parent);

	virtual void GetStorageInfo(idx_t row_group_index, vector<idx_t> col_path, vector<vector<Value>> &result);
	virtual void Verify(RowGroup &parent);

	static shared_ptr<ColumnData> CreateColumn(DataTableInfo &info, idx_t column_index, idx_t start_row, LogicalType type, ColumnData *parent = nullptr);
	static unique_ptr<ColumnData> CreateColumnUnique(DataTableInfo &info, idx_t column_index, idx_t start_row, LogicalType type, ColumnData *parent = nullptr);

protected:
	//! Append a transient segment
	void AppendTransientSegment(idx_t start_row);

	//! Scans a base vector from the column
	void ScanVector(ColumnScanState &state, Vector &result);
	//! Scans a vector from the column merged with any potential updates
	//! If ALLOW_UPDATES is set to false, the function will instead throw an exception if any updates are found
	template <bool SCAN_COMMITTED, bool ALLOW_UPDATES>
	void ScanVector(Transaction *transaction, idx_t vector_index, ColumnScanState &state, Vector &result);

protected:
	//! The segments holding the data of this column segment
	SegmentTree data;
	//! The lock for the updates
	mutex update_lock;
	//! The updates for this column segment
	unique_ptr<UpdateSegment> updates;
};

} // namespace duckdb
