
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_data_segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/segment_tree.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

class ColumnDataSegmentTree : public SegmentTree<ColumnData> {
};

} // namespace duckdb
