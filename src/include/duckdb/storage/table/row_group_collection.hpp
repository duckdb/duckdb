//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_group_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/segment_tree.hpp"
#include "duckdb/storage/statistics/column_statistics.hpp"

namespace duckdb {
struct ParallelTableScanState;

class PersistentTableData;
class TableIndexList;
class TableStatistics;

class RowGroupCollection {
public:
	RowGroupCollection(shared_ptr<DataTableInfo> info, vector<LogicalType> types, idx_t row_start,
	                   idx_t total_rows = 0);

public:
	idx_t GetTotalRows() const;
	Allocator &GetAllocator() const;

	void Initialize(PersistentTableData &data);
	void InitializeEmpty();

	void AppendRowGroup(idx_t start_row);
	void Verify();

	void InitializeScan(CollectionScanState &state, const vector<column_t> &column_ids, TableFilterSet *table_filters);
	void InitializeCreateIndexScan(CreateIndexScanState &state);
	void InitializeScanWithOffset(CollectionScanState &state, const vector<column_t> &column_ids, idx_t start_row,
	                              idx_t end_row);
	static bool InitializeScanInRowGroup(CollectionScanState &state, RowGroup *row_group, idx_t vector_index,
	                                     idx_t max_row);
	void InitializeParallelScan(ParallelCollectionScanState &state);
	bool NextParallelScan(ClientContext &context, ParallelCollectionScanState &state, CollectionScanState &scan_state);

	void Fetch(TransactionData transaction, DataChunk &result, const vector<column_t> &column_ids,
	           Vector &row_identifiers, idx_t fetch_count, ColumnFetchState &state);

	void InitializeAppend(TransactionData transaction, TableAppendState &state, idx_t append_count);
	void Append(TransactionData transaction, DataChunk &chunk, TableAppendState &state, TableStatistics &stats);
	void CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count);
	void RevertAppendInternal(idx_t start_row, idx_t count);

	void RemoveFromIndexes(TableIndexList &indexes, Vector &row_identifiers, idx_t count);

	idx_t Delete(TransactionData transaction, DataTable *table, row_t *ids, idx_t count);
	void Update(TransactionData transaction, row_t *ids, const vector<column_t> &column_ids, DataChunk &updates,
	            TableStatistics &stats);
	void UpdateColumn(TransactionData transaction, Vector &row_ids, const vector<column_t> &column_path,
	                  DataChunk &updates, TableStatistics &stats);

	void Checkpoint(TableDataWriter &writer, vector<RowGroupPointer> &row_group_pointers,
	                vector<unique_ptr<BaseStatistics>> &global_stats);

	void CommitDropColumn(idx_t index);
	void CommitDropTable();

	vector<vector<Value>> GetStorageInfo();
	const vector<LogicalType> &GetTypes() const;

	shared_ptr<RowGroupCollection> AddColumn(ColumnDefinition &new_column, Expression *default_value,
	                                         ColumnStatistics &stats);
	shared_ptr<RowGroupCollection> RemoveColumn(idx_t col_idx);
	shared_ptr<RowGroupCollection> AlterType(idx_t changed_idx, const LogicalType &target_type,
	                                         vector<column_t> bound_columns, Expression &cast_expr,
	                                         ColumnStatistics &stats);
	void VerifyNewConstraint(DataTable &parent, const BoundConstraint &constraint);

private:
	//! The number of rows in the table
	atomic<idx_t> total_rows;
	shared_ptr<DataTableInfo> info;
	vector<LogicalType> types;
	idx_t row_start;
	//! The segment trees holding the various row_groups of the table
	shared_ptr<SegmentTree> row_groups;
};

} // namespace duckdb
