//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/validity_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

//! Validity column data represents the validity data (i.e. which values are null)
class ValidityColumnData : public ColumnData {
	friend class StandardColumnData;

public:
	ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, ColumnData &parent);
	ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, ColumnDataType data_type,
	                   optional_ptr<ColumnData> parent);

public:
	//! Adapt mask-only validity decoding to the value vector produced by the parent column.
	static void ScanValidity(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void ScanPartialValidity(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                                idx_t result_offset);
	static void SelectValidity(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
	                           const SelectionVector &sel, idx_t sel_count, idx_t result_offset,
	                           ScanVectorType scan_type);

	void AppendData(ColumnAppendState &state, UnifiedVectorFormat &vdata, idx_t count) override;
	unique_ptr<ColumnCheckpointState> CreateCheckpointState(const RowGroup &row_group, Context &context,
	                                                        PartialBlockManager &partial_block_manager) override;

	void Verify(RowGroup &parent) override;
	void UpdateWithBase(TransactionData transaction, DuckTableEntry &table_entry, idx_t column_index,
	                    Vector &update_vector, row_t *row_ids, idx_t update_count, ColumnData &base,
	                    idx_t row_group_start);
};

} // namespace duckdb
