//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/numeric_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

//! A NUMERIC column stores the rows of each row group in the narrowest exact representation that fits them: an inner
//! column of a concrete type (TINYINT .. HUGEINT or DECIMAL). Appends widen the inner column when their values need
//! it, scans cast the storage type to the type the column was bound to, and the checkpoint re-picks the storage type
//! from the values of the row group.
class NumericColumnData final : public ColumnData {
public:
	//! The type of the column reference inside an expression filter - the type the filter compares against
	static LogicalType FilterColumnType(const TableFilter &filter, const LogicalType &fallback);
	NumericColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, LogicalType type,
	                  ColumnDataType data_type, optional_ptr<ColumnData> parent);

	//! The typed column holding the rows of this row group
	shared_ptr<ColumnData> GetInner() const;
	//! The representation this row group is stored in
	LogicalType GetStorageType() const;
	void SetInner(shared_ptr<ColumnData> inner);

public:
	idx_t GetMaxEntry() override;

	void InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) override;
	void InitializeScan(ColumnScanState &state) override;
	void InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) override;
	idx_t Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
	           idx_t scan_count) override;
	idx_t ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset = 0) override;
	void Skip(ColumnScanState &state, idx_t count = STANDARD_VECTOR_SIZE) override;

	FilterPropagateResult CheckZonemap(ColumnScanState &state, TableFilter &filter,
	                                   optional_ptr<SegmentNode<ColumnSegment>> &checked_segment) override;
	FilterPropagateResult CheckZonemap(optional_ptr<ClientContext> context, const StorageIndex &index,
	                                   TableFilter &filter) override;

	void InitializeAppend(ColumnAppendState &state) override;
	void Append(ColumnAppendState &state, const Vector &vector, idx_t count) override;
	void FinalizeAppend(ColumnDataFinalizeAppendState &finalize_state, ColumnAppendState &state) override;
	void RevertAppend(row_t new_count) override;

	idx_t Fetch(ColumnScanState &state, row_t row_id, Vector &result) override;
	void FetchRows(TransactionData transaction, ColumnFetchState &state, const StorageIndex &storage_index,
	               const idx_t *offsets, const SelectionVector &sel, idx_t count, Vector &result,
	               idx_t result_offset) override;

	void Update(TransactionData transaction, DuckTableEntry &table_entry, idx_t column_index, Vector &update_vector,
	            row_t *row_ids, idx_t update_count, idx_t row_group_start) override;
	void UpdateColumn(TransactionData transaction, DuckTableEntry &table_entry, const vector<column_t> &column_path,
	                  Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth,
	                  idx_t row_group_start) override;
	unique_ptr<BaseStatistics> GetUpdateStatistics() override;

	unique_ptr<ColumnCheckpointState> CreateCheckpointState(const RowGroup &row_group,
	                                                        PartialBlockManager &partial_block_manager) override;
	unique_ptr<ColumnCheckpointState> Checkpoint(const RowGroup &row_group, ColumnCheckpointInfo &info,
	                                             const BaseStatistics &old_stats) override;

	bool IsPersistent() override;
	bool HasAnyChanges() const override;
	PersistentColumnData Serialize() override;
	void InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) override;

	void GetColumnSegmentInfo(const QueryContext &context, idx_t row_group_index, vector<idx_t> col_path,
	                          vector<ColumnSegmentInfo> &result, const ColumnSegmentInfoScanOptions &options) override;
	void Verify(RowGroup &parent) override;
	void VisitBlockIds(BlockIdVisitor &visitor) const override;

	//! Cast a vector of one exact representation to another
	static void CastVector(const Vector &source, Vector &target, idx_t count);
	//! The type of the column reference inside a table filter, i.e. the type the NUMERIC column was bound to

private:
	shared_ptr<ColumnData> CreateInner(const LogicalType &storage_type);
	//! A copy of the rows of a column in a different representation
	shared_ptr<ColumnData> Rebuild(ColumnData &source, const LogicalType &new_type);
	//! Replace the inner column by a wider copy in the middle of an append
	void Widen(ColumnAppendState &state, const LogicalType &new_type);
	//! The inner column a scan state reads from - pinned when the scan was initialized so a concurrent widening does
	//! not change the column under a running scan
	ColumnData &Pinned(ColumnScanState &state);

private:
	mutable mutex inner_lock;
	shared_ptr<ColumnData> inner;
	//! Inner columns replaced by a widening, kept alive for scans that were initialized on them
	vector<shared_ptr<ColumnData>> retired;
};

} // namespace duckdb
