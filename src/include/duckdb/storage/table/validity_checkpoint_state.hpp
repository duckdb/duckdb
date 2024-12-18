//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/validity_checkpoint_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_checkpoint_state.hpp"

namespace duckdb {

struct ValidityColumnCheckpointState : public ColumnCheckpointState {
	ValidityColumnCheckpointState(RowGroup &row_group, ColumnData &column_data,
	                              PartialBlockManager &partial_block_manager, SegmentLock &&lock);

	//! The checkpoint state of the parent ColumnData, if any (struct for example doesn't have this)
	optional_ptr<ColumnCheckpointState> parent_state;
};

} // namespace duckdb
