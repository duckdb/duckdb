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
	ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row,
	                   ColumnData &parent);

public:
	unique_ptr<ColumnCheckpointState>
	CreateCheckpointState(RowGroup &row_group, PartialBlockManager &partial_block_manager, SegmentLock &&lock) override;
	FilterPropagateResult CheckZonemap(ColumnScanState &state, TableFilter &filter) override;
	void AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata, idx_t count) override;
};

} // namespace duckdb
