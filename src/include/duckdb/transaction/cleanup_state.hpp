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
#include "duckdb/transaction/commit_state.hpp"

namespace duckdb {

class DataTable;

struct DeleteInfo;
struct UpdateInfo;

class CleanupState {
public:
	explicit CleanupState(const QueryContext &context, transaction_t lowest_active_transaction,
	                      ActiveTransactionState transaction_state);

public:
	void CleanupEntry(UndoFlags type, data_ptr_t data);

private:
	QueryContext context;
	//! Lowest active transaction
	transaction_t lowest_active_transaction;
	ActiveTransactionState transaction_state;
	//! While cleaning up, we remove data from any delta indexes we added data to during the commit
	IndexDataRemover index_data_remover;

private:
	void CleanupDelete(DeleteInfo &info);
	void CleanupUpdate(UpdateInfo &info);
};

} // namespace duckdb
