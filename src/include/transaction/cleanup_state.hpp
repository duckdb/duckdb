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

	DataTable *current_table;

	// data for index cleanup
	DataChunk chunk;
	data_ptr_t data[STANDARD_VECTOR_SIZE];
	row_t row_numbers[STANDARD_VECTOR_SIZE];
	index_t count;
public:
	void CleanupEntry(UndoFlags type, data_ptr_t data);
private:
	void CleanupIndexInsert(VersionInfo *info);
	void FlushIndexCleanup();
};

}