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

namespace duckdb {
class UpdateSegment;
class TransientSegment;
class ValiditySegment;

struct ColumnAppendState {
	//! The current segment of the append
	TransientSegment *current;
	//! The update segment to append to
	UpdateSegment *updates;
	//! Child append states
	vector<ColumnAppendState> child_appends;
	//! The write lock that is held by the append
	unique_ptr<StorageLockKey> lock;
};

struct IndexLock {
	unique_lock<mutex> index_lock;
};

struct TableAppendState {
	unique_lock<mutex> append_lock;
	unique_ptr<ColumnAppendState[]> states;
	row_t row_start;
	row_t current_row;
};

} // namespace duckdb
