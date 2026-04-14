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
#include "duckdb/common/reference_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/block.hpp"

namespace duckdb {
class BlockManager;
class CatalogEntry;
class TableIndexList;
class DataChunk;
class DuckTransaction;
class WriteAheadLog;
class ClientContext;

struct DataTableInfo;
class DataTable;
struct DeleteInfo;
struct UpdateInfo;

enum class CommitMode { COMMIT, REVERT_COMMIT };

struct CommitDropAccumulator {
	struct BlockMark {
		reference<BlockManager> block_manager;
		block_id_t id;
	};
	struct IndexRemoval {
		reference<TableIndexList> indexes;
		string name;
	};

	vector<BlockMark> block_marks;
	vector<IndexRemoval> pending_index_removals;

	void AddBlock(BlockManager &bm, block_id_t id) {
		block_marks.push_back(BlockMark {bm, id});
	}
	void AddPendingIndexRemoval(TableIndexList &indexes, string name) {
		pending_index_removals.push_back(IndexRemoval {indexes, std::move(name)});
	}
	void Apply();
	void Clear() {
		block_marks.clear();
		pending_index_removals.clear();
	}
	bool Empty() const {
		return block_marks.empty() && pending_index_removals.empty();
	}
};

struct IndexDataRemover {
public:
	explicit IndexDataRemover(DuckTransaction &transaction, QueryContext context, IndexRemovalType removal_type);

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
	DataChunk chunk;
	//! Debug mode only - list of indexes to verify
	reference_map_t<DataTable, shared_ptr<DataTableInfo>> verify_indexes;
};

class CommitState {
public:
	explicit CommitState(DuckTransaction &transaction, transaction_t commit_id,
	                     ActiveTransactionState transaction_state, CommitMode commit_mode,
	                     CommitDropAccumulator &drop_accumulator);

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
	CommitDropAccumulator &drop_accumulator;
};

} // namespace duckdb
