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
class MetadataReader;

class RowGroupSegmentTree : public SegmentTree<RowGroup, true> {
public:
	RowGroupSegmentTree(RowGroupCollection &collection, idx_t base_row_id);
	~RowGroupSegmentTree() override;

	void Initialize(PersistentTableData &data);

	MetaBlockPointer GetRootPointer() const {
		return root_pointer;
	}

protected:
	shared_ptr<RowGroup> LoadSegment() const override;

	RowGroupCollection &collection;
	mutable idx_t current_row_group;
	mutable idx_t max_row_group;
	mutable unique_ptr<MetadataReader> reader;
	MetaBlockPointer root_pointer;
};

} // namespace duckdb
