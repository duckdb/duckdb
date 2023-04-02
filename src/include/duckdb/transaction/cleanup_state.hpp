//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/cleanup_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/undo_buffer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

class DataTable;

struct DeleteInfo;
struct UpdateInfo;

class CleanupState {
public:
	CleanupState();
	~CleanupState();

	// all tables with indexes that possibly need a vacuum (after e.g. a delete)
	unordered_set<DataTable *> indexed_tables;

public:
	void CleanupEntry(UndoFlags type, data_ptr_t data);

private:
	// data for index cleanup
	DataTable *current_table;
	DataChunk chunk;
	row_t row_numbers[STANDARD_VECTOR_SIZE];
	idx_t count;

private:
	void CleanupDelete(DeleteInfo *info);
	void CleanupUpdate(UpdateInfo *info);

	void Flush();
};

} // namespace duckdb
