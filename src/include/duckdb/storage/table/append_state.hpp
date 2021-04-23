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
class DataTable;
class Morsel;
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

struct MorselAppendState {
	MorselAppendState(TableAppendState &parent_p) :
		parent(parent_p) {}

	//! The parent append state
	TableAppendState &parent;
	//! The current morsel we are appending to
	Morsel *morsel;
	//! The column append states
	unique_ptr<ColumnAppendState[]> states;
	//! Offset within the morsel
	idx_t offset_in_morsel;
};


struct IndexLock {
	std::unique_lock<std::mutex> index_lock;
};

struct TableAppendState {
	TableAppendState() : morsel_append_state(*this) {}

	MorselAppendState morsel_append_state;
	std::unique_lock<std::mutex> append_lock;
	unique_ptr<IndexLock[]> index_locks;
	row_t row_start;
	row_t current_row;
	idx_t remaining_append_count;
};

} // namespace duckdb
