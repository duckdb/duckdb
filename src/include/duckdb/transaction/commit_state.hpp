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
class DuckTransaction;
class WriteAheadLog;
class ClientContext;

struct DataTableInfo;
class DataTable;
struct DeleteInfo;
struct UpdateInfo;

class CommitState {
public:
	explicit CommitState(DuckTransaction &transaction, transaction_t commit_id);
	~CommitState();

public:
	void CommitEntry(UndoFlags type, data_ptr_t data);
	void RevertCommit(UndoFlags type, data_ptr_t data);

private:
	void CommitEntryDrop(CatalogEntry &entry, data_ptr_t extra_data);
	void CommitDelete(DeleteInfo &info);
	void Flush();

private:
	DuckTransaction &transaction;
	transaction_t commit_id;
	// data for index cleanup
	optional_ptr<DataTable> current_table;
	DataChunk chunk;
	row_t row_numbers[STANDARD_VECTOR_SIZE];
	idx_t count = 0;
};

} // namespace duckdb
