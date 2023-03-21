//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_group_segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/segment_tree.hpp"
#include "duckdb/storage/table/row_group.hpp"

namespace duckdb {
struct DataTableInfo;
class PersistentTableData;
class MetaBlockReader;

class RowGroupSegmentTree : public SegmentTree<RowGroup, true> {
public:
	RowGroupSegmentTree(DataTableInfo &table_info_p, BlockManager &block_manager_p, vector<LogicalType> column_types_p);
	~RowGroupSegmentTree() override;

	void Initialize(PersistentTableData &data);

protected:
	unique_ptr<RowGroup> LoadSegment() override;

	DataTableInfo &info;
	BlockManager &block_manager;
	vector<LogicalType> column_types;
	idx_t current_row_group;
	idx_t max_row_group;
	unique_ptr<MetaBlockReader> reader;
};

} // namespace duckdb
