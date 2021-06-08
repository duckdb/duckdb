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

namespace duckdb {
class ColumnData;
class DatabaseInstance;
class RowGroup;
class TableDataWriter;

struct ColumnCheckpointState {
	ColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, TableDataWriter &writer);
	virtual ~ColumnCheckpointState();

	RowGroup &row_group;
	ColumnData &column_data;
	TableDataWriter &writer;
	SegmentTree new_tree;
	vector<DataPointer> data_pointers;
	unique_ptr<BaseStatistics> global_stats;

	unique_ptr<UncompressedSegment> current_segment;
	unique_ptr<SegmentStatistics> segment_stats;

public:
	virtual unique_ptr<BaseStatistics> GetStatistics() {
		return global_stats->Copy();
	}

	virtual void CreateEmptySegment();
	virtual void FlushSegment();
	virtual void AppendData(Vector &data, idx_t count);
	virtual void FlushToDisk();
};

} // namespace duckdb
