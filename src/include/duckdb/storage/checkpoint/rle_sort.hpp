//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/rle_sort.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/types/hyperloglog.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/checkpoint/rle_sort_options.hpp"

namespace duckdb {

struct RowGroupSortBindData : public FunctionData {
	RowGroupSortBindData(const vector<LogicalType> &payload_types, const vector<LogicalType> &keys_types,
	                     vector<column_t> indexes_p, DatabaseInstance &db_p);
	~RowGroupSortBindData() override;

	vector<LogicalType> payload_types;
	vector<LogicalType> keys_types;

	vector<column_t> indexes;

	DatabaseInstance &db;
	BufferManager &buffer_manager;
	unique_ptr<GlobalSortState> global_sort_state;
	RowLayout payload_layout;
	vector<BoundOrderByNode> orders;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

class RLESort {
public:
	RLESort(RowGroup &row_group, DataTable &data_table, vector<CompressionType> table_compression);
	void Sort();

private:
	RowGroup &row_group;
	DataTable &data_table;

	// Key Columns (i.e., columns to sort on)
	vector<LogicalType> key_column_types;
	vector<column_t> key_column_ids;

	// Payload Columns (i.e., whole table)
	vector<LogicalType> payload_column_types;
	vector<column_t> payload_column_ids;

	TableScanState scan_state;

	LocalSortState local_sort_state;
	unique_ptr<RowGroupSortBindData> sort_state;

	// Logical Types supported as keys
	bool SupportedKeyType(LogicalTypeId type_id);

	// Logical Types supported in the payload
	bool SupportedPayloadType(LogicalTypeId type_id);

	// Calculate the cardinalities with a chosen option
	void CalculateCardinalities(vector<HyperLogLog> &logs, vector<std::tuple<idx_t, idx_t>> &cardinalities,
	                            RLESortOption option);

	// Retrieve all columns with a cardinality < 500, sorted from lowest to highest cardinality
	void CardinalityBelowTenPercent(vector<HyperLogLog> &logs, vector<std::tuple<idx_t, idx_t>> &cardinalities);

	// Filter out key columns which will not be sorted on
	void FilterKeyColumns();

	// Scan the RowGroup and add key columns to HLL
	void ScanColumnsToHLL(vector<HyperLogLog> &logs);

	// Initialize key chunks, payload chunks and scan states
	void InitializeScan();

	// Initialize the sorting states
	void InitializeSort();

	// Sinks the Keys and Payloads Chunks from the row group into the sorting algorithm
	void SinkKeysPayloadSort();

	// Replaces rowgroup for the data in the sorted row group
	void ReplaceRowGroup(RowGroup &sorted_rowgroup);

	// Returns a sorted rowgroup from the sorting algorithm
	unique_ptr<RowGroup> CreateSortedRowGroup(GlobalSortState &global_sort_state);

	idx_t new_count = 0;
	idx_t old_count;
};
} // namespace duckdb