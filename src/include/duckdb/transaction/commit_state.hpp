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

namespace duckdb {
class CatalogEntry;
class DataChunk;
class WriteAheadLog;
class ClientContext;

struct DataTableInfo;
struct DeleteInfo;
struct UpdateInfo;

class CommitState {
public:
	explicit CommitState(transaction_t commit_id, optional_ptr<WriteAheadLog> log = nullptr);

	optional_ptr<WriteAheadLog> log;
	transaction_t commit_id;
	UndoFlags current_op;

	optional_ptr<DataTableInfo> current_table_info;
	idx_t row_identifiers[STANDARD_VECTOR_SIZE];

	unique_ptr<DataChunk> delete_chunk;
	unique_ptr<DataChunk> update_chunk;

public:
	template <bool HAS_LOG>
	void CommitEntry(UndoFlags type, data_ptr_t data);
	void RevertCommit(UndoFlags type, data_ptr_t data);

private:
	void SwitchTable(DataTableInfo *table, UndoFlags new_op);

	void WriteCatalogEntry(CatalogEntry &entry, data_ptr_t extra_data);
	void WriteDelete(DeleteInfo &info);
	void WriteUpdate(UpdateInfo &info);
};

} // namespace duckdb
