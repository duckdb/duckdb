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

class ColumnData {
public:
	ColumnData(DatabaseInstance &db, idx_t column_index, idx_t start_row, LogicalType type, ColumnData *parent);
	virtual ~ColumnData();

	//! The database instance this column belongs to
	DatabaseInstance &db;
	//! The column index of the column, either within the parent table or within the parent
	idx_t column_index;
	//! The start row
	idx_t start;
	//! The type of the column
	LogicalType type;
	//! The parent column (if any)
	ColumnData *parent;
	//! The updates for this column segment
	unique_ptr<UpdateSegment> updates;

public:
	virtual bool CheckZonemap(ColumnScanState &state, TableFilter &filter) = 0;

	DatabaseInstance &GetDatabase() const;

	//! The root type of the column
	const LogicalType &RootType() const;

	//! Initialize a scan of the column
	virtual void InitializeScan(ColumnScanState &state) = 0;
	//! Initialize a scan starting at the specified offset
	virtual void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) = 0;
	//! Scan the next vector from the column
	virtual void Scan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result);
	virtual void ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates);
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
	virtual void FetchRow(Transaction &transaction, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx);

	virtual void Update(Transaction &transaction, DataTableInfo &table_info, idx_t column_index, Vector &update_vector, row_t *row_ids, idx_t update_count);

	virtual void CommitDropColumn();

	virtual unique_ptr<ColumnCheckpointState> CreateCheckpointState(RowGroup &row_group, TableDataWriter &writer);
	virtual unique_ptr<ColumnCheckpointState> Checkpoint(RowGroup &row_group, TableDataWriter &writer,
	                                                     idx_t column_idx);

	virtual void Initialize(PersistentColumnData &column_data);

	static void BaseDeserialize(DatabaseInstance &db, Deserializer &source, const LogicalType &type,
	                            ColumnData &result);
	static shared_ptr<ColumnData> Deserialize(DatabaseInstance &db, idx_t column_index, idx_t start_row, Deserializer &source,
	                                          const LogicalType &type);

	virtual void Verify(RowGroup &parent);
protected:
	//! Append a transient segment
	void AppendTransientSegment(idx_t start_row);

	//! Scans a base vector from the column
	void ScanVector(ColumnScanState &state, Vector &result);
	//! Scans a vector from the column merged with any potential updates
	//! If ALLOW_UPDATES is set to false, the function will instead throw an exception if any updates are found
	template<bool SCAN_COMMITTED, bool ALLOW_UPDATES>
	void ScanVector(Transaction *transaction, idx_t vector_index, ColumnScanState &state, Vector &result);

protected:
	//! The segments holding the data of this column segment
	SegmentTree data;
};

} // namespace duckdb
