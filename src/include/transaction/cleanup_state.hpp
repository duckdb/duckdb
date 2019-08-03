//===----------------------------------------------------------------------===//
//                         DuckDB
//
// transaction/cleanup_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "transaction/undo_buffer.hpp"

namespace duckdb {

class CleanupState {
public:
	CleanupState();
	~CleanupState();

public:
	void CleanupEntry(UndoFlags type, data_ptr_t data);

private:
	// data for index cleanup
	DataTable *current_table;
	DataChunk chunk;
	data_ptr_t data[STANDARD_VECTOR_SIZE];
	row_t row_numbers[STANDARD_VECTOR_SIZE];
	index_t count;

private:
	void CleanupIndexInsert(VersionInfo *info);
	void FlushIndexCleanup();
};

} // namespace duckdb
