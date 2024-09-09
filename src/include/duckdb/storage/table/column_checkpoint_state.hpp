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
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/partial_block_manager.hpp"

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
	ColumnSegmentTree new_tree;
	vector<DataPointer> data_pointers;
	unique_ptr<BaseStatistics> global_stats;

protected:
	PartialBlockManager &partial_block_manager;

public:
	virtual unique_ptr<BaseStatistics> GetStatistics();

	virtual void FlushSegment(unique_ptr<ColumnSegment> segment, idx_t segment_size);
	virtual PersistentColumnData ToPersistentData();

	PartialBlockManager &GetPartialBlockManager() {
		return partial_block_manager;
	}

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct PartialBlockForCheckpoint : public PartialBlock {
	struct PartialColumnSegment {
		PartialColumnSegment(ColumnData &data, ColumnSegment &segment, uint32_t offset_in_block)
		    : data(data), segment(segment), offset_in_block(offset_in_block) {
		}

		ColumnData &data;
		ColumnSegment &segment;
		uint32_t offset_in_block;
	};

public:
	PartialBlockForCheckpoint(ColumnData &data, ColumnSegment &segment, PartialBlockState state,
	                          BlockManager &block_manager);
	~PartialBlockForCheckpoint() override;

	// We will copy all segment data into the memory of the shared block.
	// Once the block is full (or checkpoint is complete) we'll invoke Flush().
	// This will cause the block to get written to storage (via BlockManger::ConvertToPersistent),
	// and all segments to have their references updated (via ColumnSegment::ConvertToPersistent)
	vector<PartialColumnSegment> segments;

public:
	bool IsFlushed();
	void Flush(const idx_t free_space_left) override;
	void Merge(PartialBlock &other, idx_t offset, idx_t other_size) override;
	void AddSegmentToTail(ColumnData &data, ColumnSegment &segment, uint32_t offset_in_block);
	void Clear() override;
};

} // namespace duckdb
