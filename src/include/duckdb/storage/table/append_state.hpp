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
class TransientSegment;

struct ColumnAppendState {
	//! The current segment of the append
	TransientSegment *current;
	//! The write lock that is held by the append
	unique_ptr<StorageLockKey> lock;
};

struct IndexLock {
	std::unique_lock<std::mutex> index_lock;
};

struct TableAppendState {
	std::unique_lock<std::mutex> append_lock;
	unique_ptr<IndexLock[]> index_locks;
	unique_ptr<ColumnAppendState[]> states;
	row_t row_start;
	row_t current_row;
};

} // namespace duckdb
