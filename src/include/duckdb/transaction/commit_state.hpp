//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/commit_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/undo_buffer.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/enums/index_removal_type.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
class CatalogEntry;
class DataChunk;
class DuckTransaction;
class WriteAheadLog;
class ClientContext;

struct DataTableInfo;
class DataTable;
struct DeleteInfo;
struct UpdateInfo;

enum class CommitMode { COMMIT, REVERT_COMMIT };

struct IndexDataRemover {
public:
	explicit IndexDataRemover(DuckTransaction &transaction, QueryContext context, IndexRemovalType removal_type,
	                          optional_idx checkpoint_id);

	void PushDelete(DeleteInfo &info);
	void Verify();

private:
	void Flush(DataTable &table, row_t *row_numbers, idx_t count);

private:
	DuckTransaction &transaction;
	// data for index cleanup
	QueryContext context;
	//! While committing, we remove data from any indexes that was deleted
	IndexRemovalType removal_type;
	//! The active checkpoint id at the time of commit (captured once to ensure consistency between commit and revert)
	optional_idx checkpoint_id;
	DataChunk chunk;
	//! Debug mode only - list of indexes to verify
	reference_map_t<DataTable, shared_ptr<DataTableInfo>> verify_indexes;
};

class CommitState {
public:
	explicit CommitState(DuckTransaction &transaction, transaction_t commit_id,
	                     ActiveTransactionState transaction_state, CommitMode commit_mode, optional_idx checkpoint_id);

public:
	void CommitEntry(UndoFlags type, data_ptr_t data);
	void RevertCommit(UndoFlags type, data_ptr_t data);
	void Flush();
	void Verify();
	static IndexRemovalType GetIndexRemovalType(ActiveTransactionState transaction_state, CommitMode commit_mode);

private:
	void CommitEntryDrop(CatalogEntry &entry, data_ptr_t extra_data);
	void CommitDelete(DeleteInfo &info);

private:
	DuckTransaction &transaction;
	transaction_t commit_id;
	IndexDataRemover index_data_remover;
};

} // namespace duckdb
