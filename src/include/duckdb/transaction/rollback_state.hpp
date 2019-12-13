//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/rollback_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/undo_buffer.hpp"

namespace duckdb {
class DataChunk;
class DataTable;
class WriteAheadLog;

class RollbackState {
public:
	RollbackState() {
	}

public:
	void RollbackEntry(UndoFlags type, data_ptr_t data);
};

} // namespace duckdb
