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

struct DeleteInfo;

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
	UndoFlags flag;
	row_t row_numbers[STANDARD_VECTOR_SIZE];
	index_t count;
private:
	void CleanupDelete(DeleteInfo *info);

	void Flush();
};

} // namespace duckdb
