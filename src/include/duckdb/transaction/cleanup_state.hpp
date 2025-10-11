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
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class DataTable;

struct DeleteInfo;
struct UpdateInfo;

class CleanupState {
public:
	explicit CleanupState(const QueryContext &context, transaction_t lowest_active_transaction);
	~CleanupState();

	// all tables with indexes that possibly need a vacuum (after e.g. a delete)
	unordered_map<string, optional_ptr<DataTable>> indexed_tables;

public:
	void CleanupEntry(UndoFlags type, data_ptr_t data);

private:
	QueryContext context;
	//! Lowest active transaction
	transaction_t lowest_active_transaction;
	// data for index cleanup
	optional_ptr<DataTable> current_table;
	DataChunk chunk;
	row_t row_numbers[STANDARD_VECTOR_SIZE];
	idx_t count;

private:
	void CleanupDelete(DeleteInfo &info);
	void CleanupUpdate(UpdateInfo &info);

	void Flush();
};

} // namespace duckdb
