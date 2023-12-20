//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/storage/table/segment_tree.hpp"
#include "duckdb/storage/table/column_segment_tree.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
class ColumnData;
class ColumnSegment;
class DatabaseInstance;
class RowGroup;
class RowGroupWriter;
class TableDataWriter;
class TableStorageInfo;
struct TransactionData;

struct DataTableInfo;

struct ColumnCheckpointInfo {
	explicit ColumnCheckpointInfo(CompressionType compression_type_p) : compression_type(compression_type_p) {};
	CompressionType compression_type;
};

class ColumnData {
	friend class ColumnDataCheckpointer;

public:
	ColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row, LogicalType type,
	           optional_ptr<ColumnData> parent);
	virtual ~ColumnData();

	//! The start row
	idx_t start;
	//! The count of the column data
	idx_t count;
	//! The block manager
	BlockManager &block_manager;
	//! Table info for the column
	DataTableInfo &info;
	//! The column index of the column, either within the parent table or within the parent
	idx_t column_index;
	//! The type of the column
	LogicalType type;
	//! The parent column (if any)
	optional_ptr<ColumnData> parent;

public:
	virtual bool CheckZonemap(ColumnScanState &state, TableFilter &filter) = 0;

	BlockManager &GetBlockManager() {
		return block_manager;
	}
	DatabaseInstance &GetDatabase() const;
	DataTableInfo &GetTableInfo() const;
	virtual idx_t GetMaxEntry();

	void IncrementVersion();

	virtual void SetStart(idx_t new_start);
	//! The root type of the column
	const LogicalType &RootType() const;

	//! Initialize a scan of the column
	virtual void InitializeScan(ColumnScanState &state);
	//! Initialize a scan starting at the specified offset
	virtual void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx);
	//! Scan the next vector from the column
	virtual idx_t Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result);
	virtual idx_t ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates);
	virtual void ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t count, Vector &result);
	virtual idx_t ScanCount(ColumnScanState &state, Vector &result, idx_t count);
	//! Select
	virtual void Select(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	                    SelectionVector &sel, idx_t &count, const TableFilter &filter);
	virtual void FilterScan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	                        SelectionVector &sel, idx_t count);
	virtual void FilterScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, SelectionVector &sel,
	                                 idx_t count, bool allow_updates);

	//! Skip the scan forward by "count" rows
	virtual void Skip(ColumnScanState &state, idx_t count = STANDARD_VECTOR_SIZE);

	//! Initialize an appending phase for this column
	virtual void InitializeAppend(ColumnAppendState &state);
	//! Append a vector of type [type] to the end of the column
	virtual void Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count);
	//! Append a vector of type [type] to the end of the column
	void Append(ColumnAppendState &state, Vector &vector, idx_t count);
	virtual void AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata, idx_t count);
	//! Revert a set of appends to the ColumnData
	virtual void RevertAppend(row_t start_row);

	//! Fetch the vector from the column data that belongs to this specific row
	virtual idx_t Fetch(ColumnScanState &state, row_t row_id, Vector &result);
	//! Fetch a specific row id and append it to the vector
	virtual void FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
	                      idx_t result_idx);

	virtual void Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
	                    idx_t update_count);
	virtual void UpdateColumn(TransactionData transaction, const vector<column_t> &column_path, Vector &update_vector,
	                          row_t *row_ids, idx_t update_count, idx_t depth);
	virtual unique_ptr<BaseStatistics> GetUpdateStatistics();

	virtual void CommitDropColumn();

	virtual unique_ptr<ColumnCheckpointState> CreateCheckpointState(RowGroup &row_group,
	                                                                PartialBlockManager &partial_block_manager);
	virtual unique_ptr<ColumnCheckpointState>
	Checkpoint(RowGroup &row_group, PartialBlockManager &partial_block_manager, ColumnCheckpointInfo &checkpoint_info);

	virtual void CheckpointScan(ColumnSegment &segment, ColumnScanState &state, idx_t row_group_start, idx_t count,
	                            Vector &scan_vector);

	virtual void DeserializeColumn(Deserializer &deserializer);
	static shared_ptr<ColumnData> Deserialize(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
	                                          idx_t start_row, ReadStream &source, const LogicalType &type,
	                                          optional_ptr<ColumnData> parent);

	virtual void GetColumnSegmentInfo(idx_t row_group_index, vector<idx_t> col_path, vector<ColumnSegmentInfo> &result);
	virtual void Verify(RowGroup &parent);

	bool CheckZonemap(TableFilter &filter);

	static shared_ptr<ColumnData> CreateColumn(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
	                                           idx_t start_row, const LogicalType &type,
	                                           optional_ptr<ColumnData> parent = nullptr);
	static unique_ptr<ColumnData> CreateColumnUnique(BlockManager &block_manager, DataTableInfo &info,
	                                                 idx_t column_index, idx_t start_row, const LogicalType &type,
	                                                 optional_ptr<ColumnData> parent = nullptr);

	void MergeStatistics(const BaseStatistics &other);
	void MergeIntoStatistics(BaseStatistics &other);
	unique_ptr<BaseStatistics> GetStatistics();

protected:
	//! Append a transient segment
	void AppendTransientSegment(SegmentLock &l, idx_t start_row);

	//! Scans a base vector from the column
	idx_t ScanVector(ColumnScanState &state, Vector &result, idx_t remaining, bool has_updates);
	//! Scans a vector from the column merged with any potential updates
	//! If ALLOW_UPDATES is set to false, the function will instead throw an exception if any updates are found
	template <bool SCAN_COMMITTED, bool ALLOW_UPDATES>
	idx_t ScanVector(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result);

protected:
	//! The segments holding the data of this column segment
	ColumnSegmentTree data;
	//! The lock for the updates
	mutex update_lock;
	//! The updates for this column segment
	unique_ptr<UpdateSegment> updates;
	//! The internal version of the column data
	idx_t version;
	//! The stats of the root segment
	unique_ptr<SegmentStatistics> stats;
};

} // namespace duckdb
