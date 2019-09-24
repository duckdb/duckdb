//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/append_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "storage/storage_lock.hpp"
#include "storage/buffer/buffer_handle.hpp"


namespace duckdb {
class TransientSegment;

struct TransientAppendState {
	//! The write lock that is held by the append
	unique_ptr<StorageLockKey> lock;
	//! The handle to the current buffer that is held by the append
	unique_ptr<ManagedBufferHandle> handle;
};

struct ColumnAppendState {
	//! The current segment of the append
	TransientSegment *current;
	//! The append state
	TransientAppendState state;
};

struct TableAppendState {
	unique_ptr<std::lock_guard<std::mutex>> append_lock;
	unique_ptr<ColumnAppendState[]> states;
	row_t row_start;
	row_t current_row;
};

}
