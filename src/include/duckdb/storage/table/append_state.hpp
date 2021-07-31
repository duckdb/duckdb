//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/append_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ColumnSegment;
class DataTable;
class RowGroup;
class UpdateSegment;
class ValiditySegment;

struct TableAppendState;

struct ColumnAppendState {
	//! The current segment of the append
	ColumnSegment *current;
	//! Child append states
	vector<ColumnAppendState> child_appends;
	//! The write lock that is held by the append
	unique_ptr<StorageLockKey> lock;
};

struct RowGroupAppendState {
	RowGroupAppendState(TableAppendState &parent_p) : parent(parent_p) {
	}

	//! The parent append state
	TableAppendState &parent;
	//! The current row_group we are appending to
	RowGroup *row_group;
	//! The column append states
	unique_ptr<ColumnAppendState[]> states;
	//! Offset within the row_group
	idx_t offset_in_row_group;
};

struct IndexLock {
	unique_lock<mutex> index_lock;
};

struct TableAppendState {
	TableAppendState() : row_group_append_state(*this) {
	}

	RowGroupAppendState row_group_append_state;
	unique_lock<mutex> append_lock;
	row_t row_start;
	row_t current_row;
	idx_t remaining_append_count;
};

} // namespace duckdb
