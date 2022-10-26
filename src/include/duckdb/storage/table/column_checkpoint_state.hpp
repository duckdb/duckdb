//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_checkpoint_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
class ColumnData;
class DatabaseInstance;
class RowGroup;
class PartialBlockManager;
class TableDataWriter;

struct ColumnCheckpointState {
	ColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, PartialBlockManager &partial_block_manager);
	virtual ~ColumnCheckpointState();

	RowGroup &row_group;
	ColumnData &column_data;
	SegmentTree new_tree;
	vector<DataPointer> data_pointers;
	unique_ptr<BaseStatistics> global_stats;

protected:
	PartialBlockManager &partial_block_manager;

public:
	virtual unique_ptr<BaseStatistics> GetStatistics();

	virtual void FlushSegment(unique_ptr<ColumnSegment> segment, idx_t segment_size);
	virtual void WriteDataPointers(RowGroupWriter &writer);
	virtual void GetBlockIds(unordered_set<block_id_t> &result);
};

} // namespace duckdb
