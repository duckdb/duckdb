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
public:
	ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row,
	                   ColumnData &parent);

public:
	bool CheckZonemap(ColumnScanState &state, TableFilter &filter) override;
};

} // namespace duckdb
