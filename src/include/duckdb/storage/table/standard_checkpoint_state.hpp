//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/standard_checkpoint_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_checkpoint_state.hpp"

namespace duckdb {

struct StandardColumnCheckpointState : public ColumnCheckpointState {
	StandardColumnCheckpointState(RowGroup &row_group, ColumnData &column_data,
	                              PartialBlockManager &partial_block_manager, SegmentLock &&lock)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager, std::move(lock)) {
	}

	unique_ptr<ColumnCheckpointState> validity_state;

public:
	unique_ptr<BaseStatistics> GetStatistics() override;
	PersistentColumnData ToPersistentData() override;
};

} // namespace duckdb
