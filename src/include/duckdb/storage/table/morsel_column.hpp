//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/morsel_column.hpp
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

class MorselColumn {
public:
	MorselColumn(Morsel &morsel, LogicalType type, idx_t column_idx);

	//! The morsel this column chunk belongs to
	Morsel &morsel;
	//! The type of the column
	LogicalType type;
	//! The column index of the column
	idx_t column_idx;
	//! The root column data of this morsel column
	unique_ptr<ColumnData> column_data;
	//! Updates belonging to this morsel column (if any)
	unique_ptr<UpdateSegment> updates;
};

} // namespace duckdb
