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
class DuckTransaction;
class WriteAheadLog;

class RollbackState {
public:
	explicit RollbackState(DuckTransaction &transaction);

public:
	void RollbackEntry(UndoFlags type, data_ptr_t data);

private:
	DuckTransaction &transaction;
};

} // namespace duckdb
