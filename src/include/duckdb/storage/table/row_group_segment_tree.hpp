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

	void Initialize(PersistentTableData &data, optional_ptr<vector<MetaBlockPointer>> read_pointers = nullptr)
	    DUCKDB_REQUIRES(node_lock);

	MetaBlockPointer GetRootPointer() const {
		return root_pointer;
	}

protected:
	optional<LoadedSegment<RowGroup>> LoadSegment() const DUCKDB_REQUIRES(node_lock) override;

	RowGroupCollection &collection;
	mutable idx_t current_row_group DUCKDB_GUARDED_BY(node_lock);
	mutable idx_t max_row_group DUCKDB_GUARDED_BY(node_lock);
	mutable unique_ptr<MetadataReader> reader DUCKDB_GUARDED_BY(node_lock);
	MetaBlockPointer root_pointer;
};

} // namespace duckdb
